# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline task: SCD Type 2 dimensions (price and tier)
# MAGIC
# MAGIC Two Slowly Changing Dimensions, both built the same way: a window shapes the full version history (each
# MAGIC version's effective range and the current flag), then a single Delta `MERGE` applies it (update existing,
# MAGIC insert new), so each holds the full history even when several batches have landed at once:
# MAGIC - `silver_price_scd` from the effective dated `price_list`, one version per distinct effective date.
# MAGIC - `silver_customer_scd`, the customer dimension itself as SCD Type 2 (full attributes), built from the daily
# MAGIC   snapshots and versioned on a tier change. There is no separate current-snapshot customer table.
# MAGIC
# MAGIC Runs in parallel with Silver, or `%run` to restore the dimensions.

# COMMAND ----------

# MAGIC %run ../../00_setup/bootstrap_helpers

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, date_format, lag, lead
from pyspark.sql import Window
from delta.tables import DeltaTable

catalog = helios_identity(verbose=False)
bronze = f"{catalog}.helios_bronze"
silver = f"{catalog}.helios_silver"


# COMMAND ----------

# Step 1: build the full price history with a window. Reduce Bronze to the distinct versions, then for each
# product set effective_to to the next version's start (lead) and mark the open one current. Give each a key.
by_product = Window.partitionBy("product_id").orderBy("effective_from")
price_versions = (
    spark.read.table(f"{bronze}.bronze_price_list")
    .select("product_id", "supplier_id",
            col("unit_price").cast("decimal(12,2)").alias("unit_price"),
            col("unit_cost").cast("decimal(12,2)").alias("unit_cost"),
            "currency",
            col("effective_from").cast("date").alias("effective_from"))
    .dropDuplicates(["product_id", "effective_from"])
    .withColumn("effective_to", lead("effective_from").over(by_product))
    .withColumn("is_current", col("effective_to").isNull())
    .withColumn("price_sk", concat_ws("_", col("product_id"), date_format("effective_from", "yyyyMMdd")))
)

# Step 2: create the SCD Type 2 table if it does not exist yet.
price_versions.limit(0).write.format("delta").mode("ignore").saveAsTable(f"{silver}.silver_price_scd")

# Step 3: MERGE the versions in on price_sk: update the ones already there (so a newly closed range is
# refreshed), insert the new ones. One merge, idempotent, touching only what changed.
(DeltaTable.forName(spark, f"{silver}.silver_price_scd").alias("target")
    .merge(price_versions.alias("source"), "target.price_sk = source.price_sk")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())


# COMMAND ----------

# Step 1: build the tier history with a window. A new version starts when a customer's tier differs from the
# previous snapshot (lag); effective_to is the next version's start (lead); the open one is current.
by_customer = Window.partitionBy("customer_id").orderBy("snapshot_date")
customer_versions = (
    spark.read.table(f"{bronze}.bronze_customers")
    .select("customer_id", "customer_name", "customer_type", "tier", "home_warehouse_id",
            col("signup_date").cast("date").alias("signup_date"),
            col("snapshot_date").cast("date").alias("snapshot_date"))
    .dropDuplicates(["customer_id", "snapshot_date"])
    .withColumn("prev_tier", lag("tier").over(by_customer))
    .filter(col("prev_tier").isNull() | (col("tier") != col("prev_tier")))   # keep only the version starts
    .withColumn("effective_to", lead("snapshot_date").over(by_customer))
    .withColumn("is_current", col("effective_to").isNull())
    .withColumn("customer_sk", concat_ws("_", col("customer_id"), date_format("snapshot_date", "yyyyMMdd")))
    .withColumnRenamed("snapshot_date", "effective_from")
    .select("customer_sk", "customer_id", "customer_name", "customer_type", "tier", "home_warehouse_id",
            "signup_date", "effective_from", "effective_to", "is_current")
)

# Step 2: create the SCD Type 2 table if it does not exist yet.
customer_versions.limit(0).write.format("delta").mode("ignore").saveAsTable(f"{silver}.silver_customer_scd")

# Step 3: MERGE the versions in on customer_sk: update existing, insert new. One merge, idempotent.
(DeltaTable.forName(spark, f"{silver}.silver_customer_scd").alias("target")
    .merge(customer_versions.alias("source"), "target.customer_sk = source.customer_sk")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())
