# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline task: Silver
# MAGIC
# MAGIC Production Silver in the DataFrame and Delta Lake Python APIs:
# MAGIC - `order_lines` streams from Bronze and **upserts** on `order_line_id`, with non positive quantity or missing
# MAGIC   product rows routed to a quarantine table.
# MAGIC - `orders` applies the CDC feed with `DeltaTable.merge` (update, insert, soft delete).
# MAGIC - `returns` is typed, must point at a real line and be dated after it (orphans quarantined); it appears from
# MAGIC   batch two.
# MAGIC - `inventory` is a typed movement log, upserted on (warehouse, product, movement_ts).
# MAGIC - `products`, `categories`, `suppliers` and `warehouses` are full snapshots, so each is overwritten. (Customers are maintained as an SCD Type 2 dimension in the SCD2 task, not as a current snapshot here.)
# MAGIC
# MAGIC Upsert the append and CDC sources; overwrite the full snapshots. Re-running is safe.

# COMMAND ----------

# MAGIC %run ../../00_setup/bootstrap_helpers

# COMMAND ----------

from pyspark.sql.functions import col, row_number
from pyspark.sql import Window
from delta.tables import DeltaTable

catalog = helios_identity(verbose=False)
bronze = f"{catalog}.helios_bronze"
silver = f"{catalog}.helios_silver"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.helios_silver.checkpoints")
checkpoints = f"/Volumes/{catalog}/helios_silver/checkpoints"


def upsert(source, table, keys):
    """Create the target if needed, then MERGE the source into it on the key(s). Idempotent and incremental."""
    keys = [keys] if isinstance(keys, str) else keys
    source.limit(0).write.format("delta").mode("ignore").saveAsTable(table)
    condition = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (DeltaTable.forName(spark, table).alias("t")
        .merge(source.alias("s"), condition)
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

# COMMAND ----------

# order_lines: stream from Bronze, dedup on order_line_id, split clean vs quarantine, upsert both.
def to_silver_order_lines(micro_batch, _batch_id):
    latest = Window.partitionBy("order_line_id").orderBy(col("line_ts").desc_nulls_last())
    deduped = micro_batch.withColumn("_rn", row_number().over(latest)).filter("_rn = 1").drop("_rn")
    bad = col("quantity").isNull() | (col("quantity") <= 0) | col("product_id").isNull()
    upsert(deduped.filter(~bad), f"{silver}.silver_order_lines", "order_line_id")
    upsert(deduped.filter(bad), f"{silver}.silver_order_lines_quarantine", "order_line_id")


(spark.readStream.table(f"{bronze}.bronze_order_lines")
    .select("order_line_id", "order_id", "product_id", "customer_id", "warehouse_id",
            col("quantity").cast("int").alias("quantity"),
            col("line_ts").cast("timestamp").alias("line_ts"))
    .writeStream
    .foreachBatch(to_silver_order_lines)
    .option("checkpointLocation", f"{checkpoints}/silver_order_lines")
    .trigger(availableNow=True)
    .start().awaitTermination())

# COMMAND ----------

# orders: reduce the CDC feed to the latest change per id, then MERGE (update, insert, soft delete).
order_cols = ["order_id", "customer_id", "warehouse_id", "channel", "order_ts", "status", "change_seq", "change_ts"]
latest_change = (
    spark.read.table(f"{bronze}.bronze_orders")
    .select("order_id", "customer_id", "warehouse_id", "channel",
            col("order_ts").cast("timestamp").alias("order_ts"), "status", "op",
            col("change_seq").cast("long").alias("change_seq"),
            col("change_ts").cast("timestamp").alias("change_ts"))
    .withColumn("_rn", row_number().over(Window.partitionBy("order_id").orderBy(col("change_seq").desc())))
    .filter("_rn = 1").drop("_rn")
)
latest_change.select(*order_cols).limit(0).write.format("delta").mode("ignore").saveAsTable(f"{silver}.silver_orders")
(DeltaTable.forName(spark, f"{silver}.silver_orders").alias("t")
    .merge(latest_change.alias("s"), "t.order_id = s.order_id")
    .whenMatchedDelete(condition="s.op = 'DELETE'")
    .whenMatchedUpdate(set={c: f"s.{c}" for c in order_cols})
    .whenNotMatchedInsert(condition="s.op <> 'DELETE'", values={c: f"s.{c}" for c in order_cols})
    .execute())

# COMMAND ----------

# returns: typed; a return must point at a real line and be dated after it, else quarantine. (From batch 2.)
if spark.catalog.tableExists(f"{bronze}.bronze_returns"):
    line_ref = spark.read.table(f"{silver}.silver_order_lines").select(
        col("order_line_id").alias("_ol"), col("line_ts").alias("_line_ts"))
    returns = (
        spark.read.table(f"{bronze}.bronze_returns")
        .select("return_id", "order_line_id",
                col("return_ts").cast("timestamp").alias("return_ts"),
                col("quantity").cast("int").alias("quantity"), "reason")
        .join(line_ref, col("order_line_id") == col("_ol"), "left")
    )
    keep = col("_line_ts").isNotNull() & (col("return_ts") > col("_line_ts"))
    cols = ["return_id", "order_line_id", "return_ts", "quantity", "reason"]
    upsert(returns.filter(keep).select(*cols), f"{silver}.silver_returns", "return_id")
    upsert(returns.filter(~keep).select(*cols), f"{silver}.silver_returns_quarantine", "return_id")
    print("silver_returns built")
else:
    print("no returns landed yet (returns arrive from batch 2)")

# COMMAND ----------

# inventory: typed stock movements, upserted on (warehouse, product, movement_ts).
inventory = (
    spark.read.table(f"{bronze}.bronze_inventory")
    .select("warehouse_id", "product_id",
            col("movement_ts").cast("timestamp").alias("movement_ts"),
            col("delta").cast("int").alias("delta"),
            col("on_hand").cast("int").alias("on_hand"))
    .dropDuplicates(["warehouse_id", "product_id", "movement_ts"])
)
upsert(inventory, f"{silver}.silver_inventory", ["warehouse_id", "product_id", "movement_ts"])

# COMMAND ----------

# Reference and dimension feeds arrive as full snapshots, so each is overwritten (a replace, self healing).
def overwrite(df, table):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)


overwrite(spark.read.table(f"{bronze}.bronze_products").select(
    "product_id", "sku", "product_name", "category_id", "supplier_id",
    col("mass_kg").cast("double").alias("mass_kg"), "hazard_class",
    col("active").cast("boolean").alias("active")).dropDuplicates(["product_id"]),
    f"{silver}.silver_products")

overwrite(spark.read.table(f"{bronze}.bronze_categories").select(
    "category_id", "category_name", "department").dropDuplicates(["category_id"]),
    f"{silver}.silver_categories")

overwrite(spark.read.table(f"{bronze}.bronze_suppliers").select(
    "supplier_id", "supplier_name", "home_region",
    col("active").cast("boolean").alias("active")).dropDuplicates(["supplier_id"]),
    f"{silver}.silver_suppliers")

overwrite(spark.read.table(f"{bronze}.bronze_warehouses").select(
    "warehouse_id", "warehouse_name", "body", "region",
    col("uplink_reliability").cast("double").alias("uplink_reliability")).dropDuplicates(["warehouse_id"]),
    f"{silver}.silver_warehouses")

# customers are not built here: they are maintained as the SCD Type 2 dimension silver_customer_scd in the
# SCD2 task (there is no plain current-snapshot customer table).

print(f"Silver built for {catalog}")