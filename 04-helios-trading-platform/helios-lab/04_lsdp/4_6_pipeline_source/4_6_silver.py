# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline source: silver (Lab 4.6) -- YOUR WORKSHEET
# MAGIC
# MAGIC The layer you build in this lab. The scaffolding below is provided; write the declarations in the `TODO` cell, then return to `4_6_lab_silver` to run the pipeline and check this layer.

# COMMAND ----------

# Provided scaffolding for this pipeline source notebook (do not edit). The engine parses every notebook
# in the 4_6_pipeline_source folder together; each one reads the same student catalog from the pipeline
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

# ============================================================================
# SILVER
# ============================================================================
# Build silver_order_lines (+ expectations + quarantine), silver_inventory, the 4 ref dims, silver_returns.
# Open 4_6_lab_silver, the task, for the full instructions and the reveal solution.
#
# YOUR CODE BELOW: