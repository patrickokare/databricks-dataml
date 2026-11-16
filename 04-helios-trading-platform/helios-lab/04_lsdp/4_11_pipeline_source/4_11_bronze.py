# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline source: Bronze streaming tables (Lab 4.11)
# MAGIC
# MAGIC Part of the inherited pipeline you are on call for. The Lakeflow engine runs the four
# MAGIC `4_11_pipeline_source` notebooks together. This is a complete, running notebook; in Lab 4.11 you
# MAGIC diagnose why a night run failed and apply the fix (a one line change in `4_11_gold`).

# COMMAND ----------

# Scaffolding for this pipeline source notebook. The engine parses every notebook in the
# 4_11_pipeline_source folder together; each reads the student catalog from the pipeline configuration (`helios.catalog`) the driver passes in.
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

# Inside a pipeline current_user() is the run-as/system identity, not the developer, so read the
# catalog from the pipeline configuration the driver set, not from current_user().
catalog = spark.conf.get("helios.catalog")
landing = f"/Volumes/{catalog}/helios_raw/helios_landing"
bronze = f"{catalog}.helios_bronze"
silver = f"{catalog}.helios_silver"
gold = f"{catalog}.helios_gold"

# COMMAND ----------

# Bronze: one streaming table per feed (unchanged from Lab 4.5, with returns added now that it lands from batch
# two). The engine owns the checkpoint and schema location.
def bronze_feed(name, fmt):
    @dp.table(name=f"{bronze}.bronze_{name}",
              comment=f"Raw {name} feed, ingested from the landing Volume with Auto Loader")
    def ingest():
        reader = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", fmt)
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("rescuedDataColumn", "_rescued_data")
        )
        if fmt == "csv":
            reader = reader.option("header", "true")
        return (
            reader.load(f"{landing}/{name}")
            .withColumn("_ingest_ts", current_timestamp())
            .withColumn("_source_file", col("_metadata.file_path"))
        )
    return ingest


for feed, fmt in [
    ("order_lines", "json"), ("orders", "json"), ("returns", "json"), ("inventory", "json"),
    ("price_list", "csv"), ("customers", "parquet"), ("products", "parquet"),
    ("categories", "parquet"), ("suppliers", "json"), ("warehouses", "json"),
]:
    bronze_feed(feed, fmt)