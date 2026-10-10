# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline source: AUTO CDC and SCD Type 2 (Lab 4.11)
# MAGIC
# MAGIC Part of the inherited pipeline you are on call for. The Lakeflow engine runs the four
# MAGIC `4_11_pipeline_source` notebooks together. This is a complete, running notebook; in Lab 4.11 you
# MAGIC diagnose why a night run failed and apply the fix (a one line change in `4_11_gold`).

# COMMAND ----------

# Scaffolding for this pipeline source notebook. The engine parses every notebook in the
# 4_11_pipeline_source folder together; each reads the student catalog from the pipeline configuration (`helios.catalog`) the driver passes in.
import os
from pyspark import pipelines as dp
from pyspark.sql.functions import col, concat_ws, date_format

# Inside a pipeline current_user() is the run-as/system identity, not the developer, so read the
# catalog from the pipeline configuration the driver set, not from current_user().
catalog = spark.conf.get("helios.catalog")
landing = f"/Volumes/{catalog}/helios_raw/helios_landing"
bronze = f"{catalog}.helios_bronze"
silver = f"{catalog}.helios_silver"
gold = f"{catalog}.helios_gold"

# COMMAND ----------

# silver_orders: the order change feed reduced to current state. AUTO CDC replaces the hand rolled foreachBatch
# MERGE from Lab 3.8: one directive applies the latest change per order_id (ordered by change_seq, since
# change_ts can arrive out of order), turns a DELETE into a soft delete, and drops the CDC op column. The default
# is SCD type 1 (current state only), which is what we want here.
@dp.temporary_view()
def orders_cdc_clean():
    return (
        spark.readStream.table(f"{bronze}.bronze_orders")
        .select(
            col("order_id").cast("string").alias("order_id"),
            col("customer_id").cast("string").alias("customer_id"),
            col("warehouse_id").cast("string").alias("warehouse_id"),
            col("channel").cast("string").alias("channel"),
            col("order_ts").cast("timestamp").alias("order_ts"),
            col("status").cast("string").alias("status"),
            col("op").cast("string").alias("op"),
            col("change_seq").cast("long").alias("change_seq"),
            col("change_ts").cast("timestamp").alias("change_ts"),
        )
    )


dp.create_streaming_table(name=f"{silver}.silver_orders",
                          comment="Order current state, reduced from the CDC feed by AUTO CDC")
dp.create_auto_cdc_flow(
    target=f"{silver}.silver_orders",
    source="orders_cdc_clean",
    keys=["order_id"],
    sequence_by=col("change_seq"),          # highest change_seq per order is the current state
    apply_as_deletes=col("op") == "DELETE",  # a DELETE is a cancelled and purged order
    except_column_list=["op"],               # the CDC op flag does not belong in Silver
)


# silver_price_scd: the price and cost history as SCD Type 2, conformed to the contract columns. The price feed
# carries explicit version dates, so AUTO CDC sequences by effective_from and stores type 2 into
# silver_price_versions: one directive replaces the whole window plus MERGE from Lab 3.9. The engine adds
# __START_AT and __END_AT (same type as effective_from); the current version has a null __END_AT. A materialized
# view then maps those engine columns to the contract names (effective_from/effective_to, is_current) and builds
# the price_sk surrogate key, so the Gold facts price point in time straight off Silver with no separate price
# dimension. For example PRD-007 (the incident part) lands two versions by batch three: 7633.99 effective
# 2257-03-01, then the Ares mispricing 3435.30 effective 2257-03-03 (below its cost, so it sells at a loss).
@dp.temporary_view()
def price_cdc_clean():
    return (
        spark.readStream.table(f"{bronze}.bronze_price_list")
        .select(
            col("product_id").cast("string").alias("product_id"),
            col("supplier_id").cast("string").alias("supplier_id"),
            col("unit_price").cast("decimal(12,2)").alias("unit_price"),
            col("unit_cost").cast("decimal(12,2)").alias("unit_cost"),
            col("currency").cast("string").alias("currency"),
            col("effective_from").cast("date").alias("effective_from"),
        )
    )


dp.create_streaming_table(name=f"{silver}.silver_price_versions",
                          comment="Price and cost versions, SCD Type 2 from AUTO CDC (engine __START_AT/__END_AT)")
dp.create_auto_cdc_flow(
    target=f"{silver}.silver_price_versions",
    source="price_cdc_clean",
    keys=["product_id"],
    sequence_by=col("effective_from"),
    stored_as_scd_type=2,
)


# silver_price_scd: conform the AUTO CDC versions to the contract columns and build the price_sk surrogate key, so
# the facts join a Silver table carrying effective_from/effective_to/is_current, the same shape as the Jobs build.
@dp.materialized_view(name=f"{silver}.silver_price_scd",
                      comment="Price and cost SCD Type 2 conformed to the contract columns (with price_sk)")
def silver_price_scd():
    return (
        spark.read.table(f"{silver}.silver_price_versions")
        .select(
            concat_ws("_", col("product_id"), date_format(col("__START_AT"), "yyyyMMdd")).alias("price_sk"),
            "product_id", "supplier_id", "unit_price", "unit_cost", "currency",
            col("__START_AT").alias("effective_from"),
            col("__END_AT").alias("effective_to"),
            col("__END_AT").isNull().alias("is_current"),
        )
    )


# silver_customer_scd: the customer dimension as SCD Type 2 from the daily snapshots. The customers feed is a
# bare daily full snapshot with no version dates, so it uses the snapshot form of AUTO CDC: a function hands the
# engine each day's snapshot in order, and the engine opens a new version whenever a customer's attributes change
# (here a tier promotion). This contrast with the price feed (explicit version dates) is the teaching point. The
# version is the snapshot date as a yyyyMMdd integer, so __START_AT and __END_AT read as dates downstream.
def next_customer_snapshot(latest_version):
    # Read the daily snapshots straight from the landing Volume, not bronze_customers: a pipeline dataset
    # cannot be referenced inside this snapshot source function (it is not a dataset query definition).
    # The engine calls this once per snapshot and needs a plain Python version back, so choose the next
    # snapshot from the folder names on the Volume (customers/snapshot_<YYYY-MM-DD>), which is how the
    # Databricks examples do it. A Spark action here (collect, first, count) would run a job on the driver
    # every iteration, and pipeline source is checked for exactly that. A version is the snapshot date as a
    # yyyyMMdd integer.
    snapshot_dir = f"{landing}/customers"
    folders = {int(name.split("_", 1)[1].replace("-", "")): name
               for name in os.listdir(snapshot_dir) if name.startswith("snapshot_")}
    pending = sorted(v for v in folders if latest_version is None or v > latest_version)
    if not pending:
        return None
    version = pending[0]
    snapshot = (
        spark.read.parquet(f"{snapshot_dir}/{folders[version]}")
        .select(
            col("customer_id").cast("string").alias("customer_id"),
            col("customer_name").cast("string").alias("customer_name"),
            col("customer_type").cast("string").alias("customer_type"),
            col("tier").cast("string").alias("tier"),
            col("home_warehouse_id").cast("string").alias("home_warehouse_id"),
            col("signup_date").cast("date").alias("signup_date"),
        )
    )
    return (snapshot, version)


dp.create_streaming_table(name=f"{silver}.silver_customer_scd",
                          comment="Customer dimension as SCD Type 2 (tier history from daily snapshots)")
dp.create_auto_cdc_from_snapshot_flow(
    target=f"{silver}.silver_customer_scd",
    source=next_customer_snapshot,
    keys=["customer_id"],
    stored_as_scd_type=2,
)