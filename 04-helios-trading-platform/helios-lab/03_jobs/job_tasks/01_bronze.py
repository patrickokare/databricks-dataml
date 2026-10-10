# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline task: Bronze
# MAGIC
# MAGIC The production Bronze stage: ingest the ten source feeds from the landing Volume with Auto Loader. Pure ETL.
# MAGIC It assumes the source has already landed (the generator simulates that from a lab bootstrap, not from here).
# MAGIC `returns` arrives from batch two, so its table appears once it has landed.
# MAGIC
# MAGIC Used as a Job task, and `%run` from a later lab's bootstrap to restore Bronze.

# COMMAND ----------

# MAGIC %run ../../00_setup/bootstrap_helpers

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

catalog = helios_identity(verbose=False)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.helios_bronze.checkpoints")
chk = f"/Volumes/{catalog}/helios_bronze/checkpoints"
root = f"/Volumes/{catalog}/helios_raw/helios_landing"


def _landed(name):
    try:
        dbutils.fs.ls(f"{root}/{name}")
        return True
    except Exception:
        return False


def autoload(name, fmt):
    if not _landed(name):
        print(f"  {name}: nothing landed yet, skipping")
        return
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", fmt)
        .option("cloudFiles.schemaLocation", f"{chk}/{name}/schema")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
    )
    if fmt == "csv":
        reader = reader.option("header", "true")
    (
        reader.load(f"{root}/{name}")
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
        .writeStream
        .option("checkpointLocation", f"{chk}/{name}/chk")
        .option("mergeSchema", "true")              # absorb a new source column if the schema ever drifts
        .trigger(availableNow=True)
        .toTable(f"{catalog}.helios_bronze.bronze_{name}")
        .awaitTermination()
    )


for name, fmt in [("order_lines", "json"), ("orders", "json"), ("returns", "json"), ("inventory", "json"),
                  ("price_list", "csv"), ("customers", "parquet"), ("products", "parquet"),
                  ("categories", "parquet"), ("suppliers", "json"), ("warehouses", "json")]:
    autoload(name, fmt)

print(f"Bronze built for {catalog}")