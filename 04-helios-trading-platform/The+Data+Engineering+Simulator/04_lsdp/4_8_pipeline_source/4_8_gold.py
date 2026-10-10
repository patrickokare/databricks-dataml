# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline source: gold (Lab 4.8) -- YOUR WORKSHEET
# MAGIC
# MAGIC The layer you build in this lab. The scaffolding below is provided; write the declarations in the `TODO` cell, then return to `4_8_lab_gold` to run the pipeline and check this layer.

# COMMAND ----------

# Provided scaffolding for this pipeline source notebook (do not edit). The engine parses every notebook
# in the 4_8_pipeline_source folder together; each one reads the same student catalog from the pipeline
# configuration (`helios.catalog`) the driver passes in, then fully qualifies its tables from it.
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

# ============================================================================
# GOLD
# ============================================================================
# Build the conformed dims, dim_date, the two SCD2 dims, and the point in time facts.
# Open 4_8_lab_gold, the task, for the full instructions and the reveal solution.
#
# YOUR CODE BELOW: