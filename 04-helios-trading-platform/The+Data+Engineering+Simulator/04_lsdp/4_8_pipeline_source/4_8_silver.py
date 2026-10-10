# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline source: silver (Lab 4.8) -- provided, complete
# MAGIC
# MAGIC An earlier layer, provided complete so this lab's pipeline has its inputs. You built it in the earlier lab; you do not edit it here. This lab's worksheet is `4_8_gold`.

# COMMAND ----------

# Provided scaffolding for this pipeline source notebook (do not edit). The engine parses every notebook
# in the 4_8_pipeline_source folder together; each one reads the same student catalog from the pipeline
# configuration (`helios.catalog`) the driver passes in, then fully qualifies its tables from it.
from pyspark import pipelines as dp
from pyspark.sql.functions import col

# Inside a pipeline current_user() is the run-as/system identity, not the developer, so read the
# catalog from the pipeline configuration the driver set, not from current_user().
catalog = spark.conf.get("helios.catalog")
landing = f"/Volumes/{catalog}/helios_raw/helios_landing"
bronze = f"{catalog}.helios_bronze"
silver = f"{catalog}.helios_silver"
gold = f"{catalog}.helios_gold"

# The four return reasons, used by the warn expectation on silver_returns below.
RETURN_REASONS = "('FAULTY','WRONG_PART','NOT_NEEDED','DAMAGED_IN_TRANSIT')"

# COMMAND ----------

# silver_order_lines: typed, deduplicated, and now quality gated. Three expectations teach the three severities:
#   expect_or_fail  halts the pipeline if the primary key is ever null (it never is, by contract),
#   expect_or_drop  removes the contract breaking rows (a non positive quantity, or a missing product),
# and the dropped rows are not lost: a second flow writes them to silver_order_lines_quarantine to investigate.
@dp.table(name=f"{silver}.silver_order_lines", cluster_by=["order_line_id"],
          comment="Typed, deduplicated, quality gated order lines (one clean row per order_line_id)")
@dp.expect_or_fail("order_line_id_present", "order_line_id IS NOT NULL")
@dp.expect_or_drop("positive_quantity", "quantity > 0")
@dp.expect_or_drop("product_present", "product_id IS NOT NULL")
def silver_order_lines():
    return _typed_order_lines()


@dp.table(name=f"{silver}.silver_order_lines_quarantine",
          comment="Order lines that broke the quantity or product rule, kept for investigation")
def silver_order_lines_quarantine():
    return _typed_order_lines().filter("quantity IS NULL OR quantity <= 0 OR product_id IS NULL")


def _typed_order_lines():
    # Type every column, then deduplicate to one row per order_line_id within a 48 hour watermark. The watermark
    # removes the at least once duplicate submissions and bounds the streaming state for late arrivals.
    return (
        spark.readStream.table(f"{bronze}.bronze_order_lines")
        .select(
            col("order_line_id").cast("string").alias("order_line_id"),
            col("order_id").cast("string").alias("order_id"),
            col("product_id").cast("string").alias("product_id"),
            col("customer_id").cast("string").alias("customer_id"),
            col("warehouse_id").cast("string").alias("warehouse_id"),
            col("quantity").cast("int").alias("quantity"),
            col("line_ts").cast("timestamp").alias("line_ts"),
        )
        .withWatermark("line_ts", "48 hours")
        .dropDuplicatesWithinWatermark(["order_line_id"])
    )


# silver_inventory and the four reference dimensions: typed, one row per key.
@dp.table(name=f"{silver}.silver_inventory", comment="Typed stock movement log")
def silver_inventory():
    return (
        spark.readStream.table(f"{bronze}.bronze_inventory")
        .select(
            col("warehouse_id").cast("string").alias("warehouse_id"),
            col("product_id").cast("string").alias("product_id"),
            col("movement_ts").cast("timestamp").alias("movement_ts"),
            col("delta").cast("int").alias("delta"),
            col("on_hand").cast("int").alias("on_hand"),
        )
    )


@dp.materialized_view(name=f"{silver}.silver_products", comment="Typed product reference")
def silver_products():
    return (
        spark.read.table(f"{bronze}.bronze_products")
        .select(
            col("product_id").cast("string").alias("product_id"),
            col("sku").cast("string").alias("sku"),
            col("product_name").cast("string").alias("product_name"),
            col("category_id").cast("string").alias("category_id"),
            col("supplier_id").cast("string").alias("supplier_id"),
            col("mass_kg").cast("double").alias("mass_kg"),
            col("hazard_class").cast("string").alias("hazard_class"),
            col("active").cast("boolean").alias("active"),
        )
        .dropDuplicates(["product_id"])
    )


@dp.materialized_view(name=f"{silver}.silver_categories", comment="Typed category reference")
def silver_categories():
    return (
        spark.read.table(f"{bronze}.bronze_categories")
        .select(
            col("category_id").cast("string").alias("category_id"),
            col("category_name").cast("string").alias("category_name"),
            col("department").cast("string").alias("department"),
        )
        .dropDuplicates(["category_id"])
    )


@dp.materialized_view(name=f"{silver}.silver_suppliers", comment="Typed supplier reference")
def silver_suppliers():
    return (
        spark.read.table(f"{bronze}.bronze_suppliers")
        .select(
            col("supplier_id").cast("string").alias("supplier_id"),
            col("supplier_name").cast("string").alias("supplier_name"),
            col("home_region").cast("string").alias("home_region"),
            col("active").cast("boolean").alias("active"),
        )
        .dropDuplicates(["supplier_id"])
    )


@dp.materialized_view(name=f"{silver}.silver_warehouses", comment="Typed depot reference")
def silver_warehouses():
    return (
        spark.read.table(f"{bronze}.bronze_warehouses")
        .select(
            col("warehouse_id").cast("string").alias("warehouse_id"),
            col("warehouse_name").cast("string").alias("warehouse_name"),
            col("body").cast("string").alias("body"),
            col("region").cast("string").alias("region"),
            col("uplink_reliability").cast("double").alias("uplink_reliability"),
        )
        .dropDuplicates(["warehouse_id"])
    )


# silver_returns: returns land from batch two. A return is valid only if it points at a real order line and is
# dated after it, which is a referential rule that needs a join, so the clean and quarantine split is done by a
# filter on the join. The per row domain rules are expressions: expect_or_fail on the primary key, and a warn
# expectation on the reason enum (warn writes every row and flags violations in the event log, it does not drop).
def _returns_joined():
    returns = (
        spark.readStream.table(f"{bronze}.bronze_returns")
        .select(
            col("return_id").cast("string").alias("return_id"),
            col("order_line_id").cast("string").alias("order_line_id"),
            col("return_ts").cast("timestamp").alias("return_ts"),
            col("quantity").cast("int").alias("quantity"),
            col("reason").cast("string").alias("reason"),
        )
    )
    lines = spark.read.table(f"{silver}.silver_order_lines").select(
        col("order_line_id").alias("_ol"), col("line_ts").alias("_line_ts"))
    return returns.join(lines, col("order_line_id") == col("_ol"), "left")


@dp.table(name=f"{silver}.silver_returns", comment="Typed returns that point at a real line and are dated after it")
@dp.expect_or_fail("return_id_present", "return_id IS NOT NULL")
@dp.expect("valid_reason", f"reason IN {RETURN_REASONS}")
def silver_returns():
    cols = ["return_id", "order_line_id", "return_ts", "quantity", "reason"]
    return _returns_joined().filter(col("_line_ts").isNotNull() & (col("return_ts") > col("_line_ts"))).select(*cols)


@dp.table(name=f"{silver}.silver_returns_quarantine",
          comment="Orphan returns (no matching line) or returns dated on or before their line")
def silver_returns_quarantine():
    cols = ["return_id", "order_line_id", "return_ts", "quantity", "reason"]
    return _returns_joined().filter(col("_line_ts").isNull() | (col("return_ts") <= col("_line_ts"))).select(*cols)