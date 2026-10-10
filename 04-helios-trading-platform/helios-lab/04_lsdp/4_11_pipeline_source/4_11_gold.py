# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline source: Gold star schema (with the teammate's margin gate) (Lab 4.11)
# MAGIC
# MAGIC Part of the inherited pipeline you are on call for. The Lakeflow engine runs the four
# MAGIC `4_11_pipeline_source` notebooks together. This is a complete, running notebook; in Lab 4.11 you
# MAGIC diagnose why a night run failed and apply the fix (a one line change in `4_11_gold`).

# COMMAND ----------

# Scaffolding for this pipeline source notebook. The engine parses every notebook in the
# 4_11_pipeline_source folder together; each reads the student catalog from the pipeline configuration (`helios.catalog`) the driver passes in.
from pyspark import pipelines as dp
from pyspark.sql.functions import (col, to_date, date_format, concat_ws, year, month, dayofmonth, dayofweek, explode, sequence, expr, min as min_, max as max_)

# Inside a pipeline current_user() is the run-as/system identity, not the developer, so read the
# catalog from the pipeline configuration the driver set, not from current_user().
catalog = spark.conf.get("helios.catalog")
landing = f"/Volumes/{catalog}/helios_raw/helios_landing"
bronze = f"{catalog}.helios_bronze"
silver = f"{catalog}.helios_silver"
gold = f"{catalog}.helios_gold"

# COMMAND ----------

# Conformed dimensions and the generated date dimension are unchanged from Lab 4.5.
@dp.materialized_view(name=f"{gold}.dim_product", comment="Product dimension")
def dim_product():
    return spark.read.table(f"{silver}.silver_products").select(
        "product_id", "sku", "product_name", "category_id", "supplier_id", "mass_kg", "hazard_class", "active")


@dp.materialized_view(name=f"{gold}.dim_supplier", comment="Supplier dimension")
def dim_supplier():
    return spark.read.table(f"{silver}.silver_suppliers").select(
        "supplier_id", "supplier_name", "home_region", "active")


@dp.materialized_view(name=f"{gold}.dim_warehouse", comment="Depot dimension")
def dim_warehouse():
    return spark.read.table(f"{silver}.silver_warehouses").select(
        "warehouse_id", "warehouse_name", "body", "region", "uplink_reliability")


@dp.materialized_view(name=f"{gold}.dim_category", comment="Category dimension")
def dim_category():
    return spark.read.table(f"{silver}.silver_categories").select(
        "category_id", "category_name", "department")


@dp.materialized_view(name=f"{gold}.dim_date", comment="Generated date dimension")
def dim_date():
    dates = (
        spark.read.table(f"{silver}.silver_order_lines").select(to_date("line_ts").alias("d"))
        .union(spark.read.table(f"{silver}.silver_inventory").select(to_date("movement_ts").alias("d")))
    )
    span = dates.select(min_("d").alias("from_date"), max_("d").alias("to_date"))
    return (
        span.select(explode(sequence(col("from_date"), col("to_date"), expr("INTERVAL 1 DAY"))).alias("date"))
        .select(
            date_format("date", "yyyyMMdd").cast("int").alias("date_key"), col("date"),
            year("date").alias("year"), month("date").alias("month"), dayofmonth("date").alias("day"),
            date_format("date", "EEEE").alias("day_name"), dayofweek("date").isin(1, 7).alias("is_weekend"),
        )
    )


# dim_customer derives the contract columns from the customer SCD2 streaming table. The engine emits __START_AT
# and __END_AT, not the course's names, so here we map them: effective_from is __START_AT, effective_to is
# __END_AT, is_current is __END_AT IS NULL, and the surrogate key is <id>_<YYYYMMDD>. Price is conformed in Silver
# as silver_price_scd, so there is no Gold price dimension: the facts price point in time straight off Silver.
@dp.materialized_view(name=f"{gold}.dim_customer", comment="Customer dimension, SCD Type 2 (tier over time)")
def dim_customer():
    # The customer SCD2 versions are sequenced by the yyyyMMdd snapshot version, so __START_AT/__END_AT are that
    # integer; read them back as dates.
    return (
        spark.read.table(f"{silver}.silver_customer_scd")
        .select(
            concat_ws("_", col("customer_id"), col("__START_AT").cast("string")).alias("customer_sk"),
            "customer_id", "customer_name", "customer_type", "tier", "home_warehouse_id", "signup_date",
            to_date(col("__START_AT").cast("string"), "yyyyMMdd").alias("effective_from"),
            to_date(col("__END_AT").cast("string"), "yyyyMMdd").alias("effective_to"),
            col("__END_AT").isNull().alias("is_current"),
        )
    )


# fact_order_lines: now point in time priced on the price SCD2. Each line is valued at the price version whose
# effective range covers its line_ts (effective_from <= line_ts < effective_to, a null effective_to is open).
# The ranges are non overlapping, so the join stays one row per line, and is_returned flags the lines that come
# back. The incident day PRD-007 lines at Ares value at 3435.30 and carry a negative margin (sold below cost).
# A teammate added a strict quality gate here: the pipeline FAILS if any line has a negative margin.
# It passed for days, until the Ares mispricing landed. Lab 4.11 has you diagnose and fix this.
@dp.materialized_view(name=f"{gold}.fact_order_lines", cluster_by=["order_line_id"],
                      comment="Order line fact, point in time priced on the price SCD2")
@dp.expect_or_fail("non_negative_margin", "margin >= 0")
def fact_order_lines():
    lines = spark.read.table(f"{silver}.silver_order_lines").alias("l")
    prices = spark.read.table(f"{silver}.silver_price_scd").alias("p")
    returned = spark.read.table(f"{silver}.silver_returns").select(col("order_line_id").alias("_ret_ol")).distinct()
    return (
        lines.join(
            prices,
            (col("l.product_id") == col("p.product_id"))
            & (to_date(col("l.line_ts")) >= col("p.effective_from"))
            & (col("p.effective_to").isNull() | (to_date(col("l.line_ts")) < col("p.effective_to"))),
            "left")
        .join(returned, col("l.order_line_id") == col("_ret_ol"), "left")
        .select(
            col("l.order_line_id"), col("l.order_id"), col("l.product_id"), col("l.customer_id"),
            col("l.warehouse_id"), col("p.supplier_id"),
            date_format(col("l.line_ts"), "yyyyMMdd").cast("int").alias("date_key"),
            col("p.price_sk"), col("l.quantity"), col("p.unit_price"), col("p.unit_cost"),
            (col("l.quantity") * col("p.unit_price")).cast("decimal(14,2)").alias("gross_amount"),
            (col("l.quantity") * (col("p.unit_price") - col("p.unit_cost"))).cast("decimal(14,2)").alias("margin"),
            col("_ret_ol").isNotNull().alias("is_returned"),
        )
    )


# fact_returns: refund valued at the ORIGINAL paid price (point in time at the line's line_ts), from batch two.
@dp.materialized_view(name=f"{gold}.fact_returns", comment="Return fact, refund at the original paid price")
def fact_returns():
    rets = spark.read.table(f"{silver}.silver_returns").alias("r")
    lines = spark.read.table(f"{silver}.silver_order_lines").alias("l")
    prices = spark.read.table(f"{silver}.silver_price_scd").alias("p")
    return (
        rets.join(lines, col("r.order_line_id") == col("l.order_line_id"), "inner")
        .join(prices,
              (col("l.product_id") == col("p.product_id"))
              & (to_date(col("l.line_ts")) >= col("p.effective_from"))
              & (col("p.effective_to").isNull() | (to_date(col("l.line_ts")) < col("p.effective_to"))),
              "left")
        .select(
            col("r.return_id"), col("r.order_line_id"), col("l.product_id"), col("l.customer_id"),
            col("l.warehouse_id"),
            date_format(col("r.return_ts"), "yyyyMMdd").cast("int").alias("date_key"),
            col("r.quantity").alias("quantity_returned"),
            (col("r.quantity") * col("p.unit_price")).cast("decimal(14,2)").alias("refund_amount"),
        )
    )


# fact_inventory: unchanged from Lab 4.5.
@dp.materialized_view(name=f"{gold}.fact_inventory", comment="Stock movement fact")
def fact_inventory():
    return spark.read.table(f"{silver}.silver_inventory").select(
        "warehouse_id", "product_id", "movement_ts",
        date_format("movement_ts", "yyyyMMdd").cast("int").alias("date_key"), "delta", "on_hand")