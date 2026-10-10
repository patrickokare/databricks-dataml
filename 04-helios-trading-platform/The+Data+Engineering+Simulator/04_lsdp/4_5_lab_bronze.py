# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 4.5: Declare the bronze streaming tables
# MAGIC
# MAGIC ### Ticket: HELIOS-405
# MAGIC
# MAGIC **Context.** Section 4 rebuilds the Helios medallion declaratively with Lakeflow Spark Declarative Pipelines, one layer per lab, the same split as the Section 3 job tasks. In this lab you build the **bronze** layer. You author the declarations in the source notebook `4_5_bronze` (in the `4_5_pipeline_source` folder), then run the one pipeline from here and check the result.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bootstrap
# MAGIC Resets the layers the pipeline owns and lands batches one to three, then creates the one pipeline pointing at this lab's source notebooks (4_5_bronze). Gold lands in `helios_gold`, the one canonical Gold schema, the same target the Section 3 Jobs build writes. Run this top to bottom before the task.

# COMMAND ----------

# MAGIC %run ../00_setup/data_generator

# COMMAND ----------

# MAGIC %run ../00_setup/bootstrap_helpers

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import pipelines
from pyspark.sql.functions import col
import time

catalog = helios_identity()
w = WorkspaceClient()

# Clean slate, then land batches one to three (the full data every layer needs: CDC history, the price
# incident, tier promotions and returns). One pipeline builds all three layers, and Gold goes to the one
# canonical helios_gold schema, the same target the Section 3 Jobs build writes, so reset all three.
for layer in ["helios_bronze", "helios_silver"]:
    reset_schema(catalog, layer)
spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.helios_gold CASCADE")
spark.sql(f"CREATE SCHEMA {catalog}.helios_gold")
clear_landing(catalog)
ensure_landing(catalog, up_to_batch=3)

# This lab's pipeline source notebooks (earlier layers provided complete; this layer is your worksheet).
_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
_dir = _ctx.notebookPath().get().rsplit("/", 1)[0]
src_dir = f"{_dir}/4_5_pipeline_source"
source_notebooks = ["4_5_bronze"]
pipeline_name = f"helios_lsdp_{catalog}"


def find_pipeline():
    matches = w.pipelines.list_pipelines(filter=f"name LIKE '{pipeline_name}'")
    return next((p for p in matches if p.name == pipeline_name), None)


def run_pipeline():
    """Trigger an update on the pipeline and wait for it to finish. Returns the terminal state."""
    pid = find_pipeline().pipeline_id
    update_id = w.pipelines.start_update(pipeline_id=pid).update_id
    terminal = {"COMPLETED", "FAILED", "CANCELED"}
    while True:
        update = w.pipelines.get_update(pipeline_id=pid, update_id=update_id).update
        state = update.state.value if update.state else "PENDING"
        if state in terminal:
            return state
        time.sleep(15)


# One pipeline per type on Free Edition, so replace any existing pipeline of this name, then create one
# pointing at this lab's source notebooks (the engine parses them together and wires the graph by name).
existing = find_pipeline()
if existing:
    w.pipelines.delete(pipeline_id=existing.pipeline_id)
libraries = [pipelines.PipelineLibrary(notebook=pipelines.NotebookLibrary(path=f"{src_dir}/{nb}"))
             for nb in source_notebooks]
pipeline_id = w.pipelines.create(
    name=pipeline_name, serverless=True, continuous=False,
    catalog=catalog, schema="helios_bronze",
    configuration={"helios.catalog": catalog},
    libraries=libraries,
).pipeline_id
print(f"Created pipeline {pipeline_id}: {pipeline_name}, pointing at {len(source_notebooks)} source notebook(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC Open `4_5_bronze` in the `4_5_pipeline_source` folder. It opens with a provided scaffolding cell (do not edit) that imports the pipeline API and reads your catalog from the `helios.catalog` value this driver passes into the pipeline configuration, so you write only the table declarations in its `TODO` cell. Then run the check cell below: it triggers a pipeline run and validates this layer. A run takes a couple of minutes on serverless. The reveal has the solution if you get stuck.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your task: Declare the bronze streaming tables
# MAGIC
# MAGIC In `4_5_bronze`, declare one bronze streaming table per source feed, each ingesting its landing folder with Auto
# MAGIC Loader and adding ingest metadata, so every raw row lands once and stays traceable. The `@dp.table` that takes a
# MAGIC few lines here replaces the forty line imperative Auto Loader stream from Lab 3.7, because the engine owns the
# MAGIC checkpoint and the schema location.
# MAGIC
# MAGIC **How.**
# MAGIC - Declare each feed as a streaming table with `@dp.table`, returning a `cloudFiles` (Auto Loader) read of its
# MAGIC   folder under the landing Volume. Turn on schema evolution (`addNewColumns`) and rescue unexpected values into
# MAGIC   `_rescued_data`, and add `_ingest_ts` and `_source_file` to every row.
# MAGIC - The ten feeds and formats: order_lines, orders, returns, inventory, suppliers and warehouses are JSON;
# MAGIC   price_list is CSV with a header; customers, products and categories are Parquet.
# MAGIC - A pipeline dataset function only returns the DataFrame: do not call `writeStream`, set a checkpoint, or `start`.
# MAGIC
# MAGIC **Example.** A `bronze_order_lines` row for `OL-1-000001` carries the raw columns plus a non null `_ingest_ts`
# MAGIC and a `_source_file` ending `order_lines/batch_1/part-00000-....json`.
# MAGIC
# MAGIC **Definition of done.**
# MAGIC - The pipeline run completes when you call `run_pipeline()`.
# MAGIC - All ten bronze streaming tables exist in `helios_bronze`: `bronze_order_lines`, `bronze_orders`, `bronze_returns`, `bronze_inventory`, `bronze_price_list`, `bronze_customers`, `bronze_products`, `bronze_categories`, `bronze_suppliers` and `bronze_warehouses`.
# MAGIC - `bronze_order_lines` reconciles to the landing folder: its row count equals the number of records in the landed `order_lines` JSON files under `helios_raw/helios_landing/order_lines`.
# MAGIC - `bronze_order_lines` carries the ingest metadata columns `_ingest_ts`, `_source_file` and `_rescued_data`.
# MAGIC
# MAGIC **References.** You may find these Databricks docs helpful:
# MAGIC - [Streaming tables](https://docs.databricks.com/aws/en/dlt/streaming-tables)
# MAGIC - [Load data in pipelines](https://docs.databricks.com/aws/en/ldp/load)
# MAGIC - [Configure schema inference and evolution in Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema)
# MAGIC - [File metadata column](https://docs.databricks.com/aws/en/ingestion/file-metadata-column)

# COMMAND ----------

# Your turn. Write the bronze declarations in 4_5_pipeline_source/4_5_bronze, then run the check cell.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Declare the bronze streaming tables (solution)
# MAGIC <details>
# MAGIC <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; overflow:auto;"></pre>
# MAGIC
# MAGIC <script>
# MAGIC var codeB64 = "IyBCcm9uemU6IG9uZSBzdHJlYW1pbmcgdGFibGUgcGVyIGZlZWQsIGluZ2VzdGVkIGZyb20gdGhlIGxhbmRpbmcgVm9sdW1lIHdpdGggQXV0byBMb2FkZXIuIFRoZSBlbmdpbmUgb3ducyB0aGUgY2hlY2twb2ludCBhbmQgc2NoZW1hIGxvY2F0aW9uLgpkZWYgYnJvbnplX2ZlZWQobmFtZSwgZm10KToKICAgIEBkcC50YWJsZShuYW1lPWYie2Jyb256ZX0uYnJvbnplX3tuYW1lfSIsCiAgICAgICAgICAgICAgY29tbWVudD1mIlJhdyB7bmFtZX0gZmVlZCwgaW5nZXN0ZWQgZnJvbSB0aGUgbGFuZGluZyBWb2x1bWUgd2l0aCBBdXRvIExvYWRlciIpCiAgICBkZWYgaW5nZXN0KCk6CiAgICAgICAgcmVhZGVyID0gKAogICAgICAgICAgICBzcGFyay5yZWFkU3RyZWFtLmZvcm1hdCgiY2xvdWRGaWxlcyIpCiAgICAgICAgICAgIC5vcHRpb24oImNsb3VkRmlsZXMuZm9ybWF0IiwgZm10KQogICAgICAgICAgICAub3B0aW9uKCJjbG91ZEZpbGVzLnNjaGVtYUV2b2x1dGlvbk1vZGUiLCAiYWRkTmV3Q29sdW1ucyIpCiAgICAgICAgICAgIC5vcHRpb24oInJlc2N1ZWREYXRhQ29sdW1uIiwgIl9yZXNjdWVkX2RhdGEiKQogICAgICAgICkKICAgICAgICBpZiBmbXQgPT0gImNzdiI6CiAgICAgICAgICAgIHJlYWRlciA9IHJlYWRlci5vcHRpb24oImhlYWRlciIsICJ0cnVlIikKICAgICAgICByZXR1cm4gKAogICAgICAgICAgICByZWFkZXIubG9hZChmIntsYW5kaW5nfS97bmFtZX0iKQogICAgICAgICAgICAud2l0aENvbHVtbigiX2luZ2VzdF90cyIsIGN1cnJlbnRfdGltZXN0YW1wKCkpCiAgICAgICAgICAgIC53aXRoQ29sdW1uKCJfc291cmNlX2ZpbGUiLCBjb2woIl9tZXRhZGF0YS5maWxlX3BhdGgiKSkKICAgICAgICApCiAgICByZXR1cm4gaW5nZXN0CgoKZm9yIGZlZWQsIGZtdCBpbiBbCiAgICAoIm9yZGVyX2xpbmVzIiwgImpzb24iKSwgKCJvcmRlcnMiLCAianNvbiIpLCAoInJldHVybnMiLCAianNvbiIpLCAoImludmVudG9yeSIsICJqc29uIiksCiAgICAoInByaWNlX2xpc3QiLCAiY3N2IiksICgiY3VzdG9tZXJzIiwgInBhcnF1ZXQiKSwgKCJwcm9kdWN0cyIsICJwYXJxdWV0IiksCiAgICAoImNhdGVnb3JpZXMiLCAicGFycXVldCIpLCAoInN1cHBsaWVycyIsICJqc29uIiksICgid2FyZWhvdXNlcyIsICJqc29uIiksCl06CiAgICBicm9uemVfZmVlZChmZWVkLCBmbXQp";
# MAGIC var codeText = atob(codeB64);
# MAGIC var box = document.getElementById("copy-block");
# MAGIC if (box) { box.textContent = codeText; }
# MAGIC function copyBlock() {
# MAGIC   var text = document.getElementById("copy-block").textContent;
# MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
# MAGIC     navigator.clipboard.writeText(text).then(function () { alert("Copied to clipboard"); })
# MAGIC       .catch(function (err) { console.error("Clipboard write failed:", err); fallbackCopy(text); });
# MAGIC   } else { fallbackCopy(text); }
# MAGIC }
# MAGIC function fallbackCopy(text) {
# MAGIC   var ta = document.createElement("textarea");
# MAGIC   ta.value = text; ta.style.position = "fixed"; ta.style.left = "-9999px";
# MAGIC   document.body.appendChild(ta); ta.select();
# MAGIC   try { document.execCommand("copy"); alert("Copied to clipboard"); }
# MAGIC   catch (err) { console.error("Fallback copy failed:", err); alert("Could not copy to clipboard. Please copy manually."); }
# MAGIC   finally { document.body.removeChild(ta); }
# MAGIC }
# MAGIC </script>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run and check
# MAGIC Confirms the bronze tables exist and reconcile to the landed files.

# COMMAND ----------

print("Pipeline run:", run_pipeline())

bronze = f"{catalog}.helios_bronze"
feeds = ["order_lines", "orders", "returns", "inventory", "price_list", "customers",
         "products", "categories", "suppliers", "warehouses"]
check_true("all ten bronze tables exist",
           all(spark.catalog.tableExists(f"{bronze}.bronze_{f}") for f in feeds))
landed = (spark.read.option("recursiveFileLookup", "true")
          .json(f"/Volumes/{catalog}/helios_raw/helios_landing/order_lines").count())
check("bronze_order_lines ingested every landed order line", landed,
      spark.table(f"{bronze}.bronze_order_lines").count())
check_true("bronze rows carry the ingest metadata",
           {"_ingest_ts", "_source_file", "_rescued_data"}.issubset(
               set(spark.table(f"{bronze}.bronze_order_lines").columns)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What you built
# MAGIC
# MAGIC The **bronze** layer of the Helios medallion, declared in Lakeflow and built by the engine. You wrote the declarations, ran the one pipeline, and validated this layer from here.