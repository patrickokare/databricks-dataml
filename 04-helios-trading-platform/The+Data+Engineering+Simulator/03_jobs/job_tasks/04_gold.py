# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline task: Gold
# MAGIC
# MAGIC The Gold star schema in the DataFrame and Delta Lake Python APIs. The conformed dimensions are full snapshots
# MAGIC from Silver, so each is **overwritten** (including `dim_customer`, which is the customer SCD Type 2). The three **facts** are **upserted** on their business keys, costed by
# MAGIC a point in time join to the price SCD2:
# MAGIC - `fact_order_lines` (one row per line): priced and costed as of `line_ts`, with `gross_amount` and `margin`.
# MAGIC - `fact_returns` (one row per return): refund valued at the original paid price (from batch 2).
# MAGIC - `fact_inventory` (one row per stock movement): the signed `delta` and the running `on_hand`.
# MAGIC
# MAGIC Runs after Silver and the SCD2 task, or `%run` to restore Gold.

# COMMAND ----------

# MAGIC %run ../../00_setup/bootstrap_helpers

# COMMAND ----------

from pyspark.sql.functions import (
    col, to_date, date_format, lit, year, month, dayofmonth, dayofweek, explode, sequence, expr,
    coalesce, min as min_, max as max_,
)
from delta.tables import DeltaTable

catalog = helios_identity(verbose=False)
silver = f"{catalog}.helios_silver"
gold = f"{catalog}.helios_gold"


def upsert(source, table, keys):
    """Create the target if needed, then MERGE the source into it on the key(s). Idempotent and incremental."""
    keys = [keys] if isinstance(keys, str) else keys
    source.limit(0).write.format("delta").mode("ignore").saveAsTable(table)
    condition = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (DeltaTable.forName(spark, table).alias("t")
        .merge(source.alias("s"), condition)
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

# COMMAND ----------

# Conformed dimensions are full snapshots from Silver, so each is overwritten (a self healing replace).
dimensions = [
    ("silver_products", "dim_product",
     ["product_id", "sku", "product_name", "category_id", "supplier_id", "mass_kg", "hazard_class", "active"]),
    ("silver_customer_scd", "dim_customer",
     ["customer_sk", "customer_id", "customer_name", "customer_type", "tier", "home_warehouse_id",
      "signup_date", "effective_from", "effective_to", "is_current"]),
    ("silver_suppliers", "dim_supplier", ["supplier_id", "supplier_name", "home_region", "active"]),
    ("silver_warehouses", "dim_warehouse",
     ["warehouse_id", "warehouse_name", "body", "region", "uplink_reliability"]),
    ("silver_categories", "dim_category", ["category_id", "category_name", "department"]),
]
for src, dst, cols in dimensions:
    (spark.read.table(f"{silver}.{src}").select(*cols)
        .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{gold}.{dst}"))

# dim_date over the union of the line and movement date ranges.
dates = (
    spark.read.table(f"{silver}.silver_order_lines").select(to_date("line_ts").alias("d"))
    .union(spark.read.table(f"{silver}.silver_inventory").select(to_date("movement_ts").alias("d")))
)
span = dates.select(min_("d").alias("from_date"), max_("d").alias("to_date"))
dim_date = (
    span.select(explode(sequence(col("from_date"), col("to_date"), expr("INTERVAL 1 DAY"))).alias("date"))
    .select(date_format("date", "yyyyMMdd").cast("int").alias("date_key"), col("date"),
            year("date").alias("year"), month("date").alias("month"), dayofmonth("date").alias("day"),
            date_format("date", "EEEE").alias("day_name"), dayofweek("date").isin(1, 7).alias("is_weekend"))
)
dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{gold}.dim_date")

# COMMAND ----------

# fact_order_lines: point in time priced and costed, with gross_amount and margin, upserted on order_line_id.
lines = spark.read.table(f"{silver}.silver_order_lines").alias("l")
prices = spark.read.table(f"{silver}.silver_price_scd").alias("p")
# order_status: the order's current lifecycle status (a degenerate attribute), from the current-state
# silver_orders, left joined on order_id (one row per order there, so no fan out). It is NULL when the order
# has no current record: its CDC change has not arrived yet, or it was a purged (soft deleted) cancellation.
orders = spark.read.table(f"{silver}.silver_orders").select(
    col("order_id").alias("_ord_id"), col("status").alias("order_status"))
fact = (
    lines.join(
        prices,
        (col("l.product_id") == col("p.product_id"))
        & (to_date(col("l.line_ts")) >= col("p.effective_from"))
        & (col("p.effective_to").isNull() | (to_date(col("l.line_ts")) < col("p.effective_to"))),
        "left")
    .join(orders, col("l.order_id") == col("_ord_id"), "left")
    .select(col("l.order_line_id"), col("l.order_id"), col("l.product_id"), col("l.customer_id"),
            col("l.warehouse_id"), col("p.supplier_id"),
            date_format(col("l.line_ts"), "yyyyMMdd").cast("int").alias("date_key"),
            col("p.price_sk"), col("l.quantity"), col("p.unit_price"), col("p.unit_cost"),
            (col("l.quantity") * col("p.unit_price")).cast("decimal(14,2)").alias("gross_amount"),
            (col("l.quantity") * (col("p.unit_price") - col("p.unit_cost"))).cast("decimal(14,2)").alias("margin"),
            col("order_status"))
)
# is_returned: the line appears in silver_returns (which exists from batch 2).
if spark.catalog.tableExists(f"{silver}.silver_returns"):
    returned = spark.read.table(f"{silver}.silver_returns").select("order_line_id").distinct().withColumn("_ret", lit(True))
    fact = fact.join(returned, "order_line_id", "left").withColumn("is_returned", coalesce(col("_ret"), lit(False))).drop("_ret")
else:
    fact = fact.withColumn("is_returned", lit(False))
upsert(fact, f"{gold}.fact_order_lines", "order_line_id")

# COMMAND ----------

# fact_returns: refund valued at the ORIGINAL paid price (point in time at the line), upserted on return_id.
if spark.catalog.tableExists(f"{silver}.silver_returns"):
    rets = spark.read.table(f"{silver}.silver_returns").alias("r")
    line2 = spark.read.table(f"{silver}.silver_order_lines").alias("l")
    price2 = spark.read.table(f"{silver}.silver_price_scd").alias("p")
    fact_r = (
        rets.join(line2, col("r.order_line_id") == col("l.order_line_id"), "inner")
        .join(price2,
              (col("l.product_id") == col("p.product_id"))
              & (to_date(col("l.line_ts")) >= col("p.effective_from"))
              & (col("p.effective_to").isNull() | (to_date(col("l.line_ts")) < col("p.effective_to"))),
              "left")
        .select(col("r.return_id"), col("r.order_line_id"), col("l.product_id"), col("l.customer_id"),
                col("l.warehouse_id"),
                date_format(col("r.return_ts"), "yyyyMMdd").cast("int").alias("date_key"),
                col("r.quantity").alias("quantity_returned"),
                (col("r.quantity") * col("p.unit_price")).cast("decimal(14,2)").alias("refund_amount"))
    )
    upsert(fact_r, f"{gold}.fact_returns", "return_id")
    print("fact_returns built")
else:
    print("no returns yet (fact_returns builds from batch 2)")

# COMMAND ----------

# fact_inventory: stock movements at the (warehouse, product, movement_ts) grain.
movements = spark.read.table(f"{silver}.silver_inventory").select(
    "warehouse_id", "product_id", "movement_ts",
    date_format("movement_ts", "yyyyMMdd").cast("int").alias("date_key"), "delta", "on_hand")
upsert(movements, f"{gold}.fact_inventory", ["warehouse_id", "product_id", "movement_ts"])

print(f"Gold built for {catalog}")