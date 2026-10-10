# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 4.8: Build the point in time gold star schema
# MAGIC
# MAGIC ### Ticket: HELIOS-408
# MAGIC
# MAGIC **Context.** Section 4 rebuilds the Helios medallion declaratively with Lakeflow Spark Declarative Pipelines, one layer per lab, the same split as the Section 3 job tasks. In this lab you build the **gold** layer. The earlier layers (bronze, silver, scd) are provided complete in the source folder, so the pipeline has its inputs. You author the declarations in the source notebook `4_8_gold` (in the `4_8_pipeline_source` folder), then run the one pipeline from here and check the result.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bootstrap
# MAGIC Resets the layers the pipeline owns and lands batches one to three, then creates the one pipeline pointing at this lab's source notebooks (4_8_bronze, 4_8_silver, 4_8_scd, 4_8_gold). Gold lands in `helios_gold`, the one canonical Gold schema, the same target the Section 3 Jobs build writes. Run this top to bottom before the task.

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
src_dir = f"{_dir}/4_8_pipeline_source"
source_notebooks = ["4_8_bronze", "4_8_silver", "4_8_scd", "4_8_gold"]
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
# MAGIC Open `4_8_gold` in the `4_8_pipeline_source` folder. It opens with a provided scaffolding cell (do not edit) that imports the pipeline API and reads your catalog from the `helios.catalog` value this driver passes into the pipeline configuration, so you write only the table declarations in its `TODO` cell. Then run the check cell below: it triggers a pipeline run and validates this layer. A run takes a couple of minutes on serverless. The reveal has the solution if you get stuck.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your task: Build the point in time gold star schema
# MAGIC
# MAGIC In `4_8_gold`, build the gold star schema as materialized views: the conformed dimensions, a generated date
# MAGIC dimension, the two SCD Type 2 dimensions, and the facts valued by a point in time price join.
# MAGIC
# MAGIC The target column names and types for every table are in `4_3_pipeline_design.md`, in this folder, under Gold schemas.
# MAGIC
# MAGIC **How.**
# MAGIC - `dim_product`, `dim_supplier`, `dim_warehouse`, `dim_category`: project the matching silver reference table.
# MAGIC   `dim_date`: a generated calendar with `date_key` as the `yyyyMMdd` integer.
# MAGIC - `dim_customer`: read the customer SCD Type 2 streaming table and map the engine's columns to the contract
# MAGIC   names, `effective_from`/`effective_to` from `__START_AT`/`__END_AT`, `is_current` from `__END_AT IS NULL`, and
# MAGIC   the surrogate key as `<id>_<YYYYMMDD>`. (Price is conformed in Silver as `silver_price_scd`, so there is no Gold
# MAGIC   price dimension; the facts price point in time off it.)
# MAGIC - `fact_order_lines`: join each line to the price version whose range covers its `line_ts` (`effective_from <=
# MAGIC   line_ts < effective_to`, a null `effective_to` open), carrying `unit_price`, `unit_cost`, `gross_amount`,
# MAGIC   `margin` and `price_sk`, and flag `is_returned` by a join to `silver_returns`. The ranges do not overlap, so it
# MAGIC   stays one row per line. Also carry the order's current lifecycle status as `order_status` (a degenerate attribute from `silver_orders` current state, NULL for purged or not yet arrived orders), which makes this an explicit bookings grain and lets cancellations be netted out downstream. `fact_returns` refunds at the original paid price; `fact_inventory` at the movement
# MAGIC   grain.
# MAGIC
# MAGIC **Example.** The canonical line `OL-1-000001` (product `PRD-017`, quantity `2`, placed `2257-03-01`) values point
# MAGIC in time to `unit_price` `10184.99`, `gross_amount` `20369.98`, `price_sk` `PRD-017_22570301`, regardless of any
# MAGIC later price change.
# MAGIC
# MAGIC **Definition of done.**
# MAGIC - The conformed dimensions exist in `helios_gold`: `dim_product`, `dim_supplier`, `dim_warehouse` and `dim_category`, each projected from its silver reference table.
# MAGIC - `dim_date` exists as a generated calendar with `date_key` as the `yyyyMMdd` integer.
# MAGIC - The facts price point in time off `silver_price_scd` (conformed in Silver from the AUTO CDC `silver_price_versions`), which carries `effective_from`/`effective_to`, `is_current` and `price_sk`; there is no Gold price dimension.
# MAGIC - `dim_customer` exists as the SCD Type 2 customer dimension (tier over time), with `customer_sk` as `<customer_id>_<yyyyMMdd>`.
# MAGIC - `fact_order_lines` is one row per line: its row count equals its distinct `order_line_id` count (no fan out), and equals the clean `silver_order_lines` row count.
# MAGIC - Every line in `fact_order_lines` is priced by the point in time join: zero rows have `price_sk IS NULL`.
# MAGIC - The canonical line `OL-1-000001` values point in time to `unit_price` `10184.99`.
# MAGIC - The canonical line `OL-1-000001` carries `price_sk` `PRD-017_22570301`, the 2257-03-01 price version.
# MAGIC - `fact_order_lines` also carries `unit_cost`, `gross_amount`, `margin` and an `is_returned` flag from the join to `silver_returns`.
# MAGIC - `fact_order_lines` carries `order_status` (the order's current lifecycle status from `silver_orders`), either NULL or one of the eight statuses.
# MAGIC - `fact_returns` is one row per return: its row count equals its distinct `return_id` count.
# MAGIC - Every return in `fact_returns` has a `refund_amount` (zero rows with `refund_amount IS NULL`), valued at the original paid price.
# MAGIC - `fact_inventory` exists at the stock movement grain, one row per movement with the signed `delta` and running `on_hand`.
# MAGIC
# MAGIC **References.** You may find these Databricks docs helpful:
# MAGIC - [Materialized views](https://docs.databricks.com/aws/en/dlt/materialized-views)
# MAGIC - [Point-in-time feature joins](https://docs.databricks.com/aws/en/machine-learning/feature-store/time-series)
# MAGIC - [The AUTO CDC APIs: Simplify change data capture with pipelines](https://docs.databricks.com/aws/en/ldp/cdc)
# MAGIC - [Range join optimization](https://docs.databricks.com/aws/en/optimizations/range-join)

# COMMAND ----------

# Your turn. Write the gold declarations in 4_8_pipeline_source/4_8_gold, then run the check cell.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Build the point in time gold star schema (solution)
# MAGIC <details>
# MAGIC <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; overflow:auto;"></pre>
# MAGIC
# MAGIC <script>
# MAGIC var codeB64 = "IyBDb25mb3JtZWQgZGltZW5zaW9ucyBhbmQgdGhlIGdlbmVyYXRlZCBkYXRlIGRpbWVuc2lvbi4KQGRwLm1hdGVyaWFsaXplZF92aWV3KG5hbWU9ZiJ7Z29sZH0uZGltX3Byb2R1Y3QiLCBjb21tZW50PSJQcm9kdWN0IGRpbWVuc2lvbiIpCmRlZiBkaW1fcHJvZHVjdCgpOgogICAgcmV0dXJuIHNwYXJrLnJlYWQudGFibGUoZiJ7c2lsdmVyfS5zaWx2ZXJfcHJvZHVjdHMiKS5zZWxlY3QoCiAgICAgICAgInByb2R1Y3RfaWQiLCAic2t1IiwgInByb2R1Y3RfbmFtZSIsICJjYXRlZ29yeV9pZCIsICJzdXBwbGllcl9pZCIsICJtYXNzX2tnIiwgImhhemFyZF9jbGFzcyIsICJhY3RpdmUiKQoKCkBkcC5tYXRlcmlhbGl6ZWRfdmlldyhuYW1lPWYie2dvbGR9LmRpbV9zdXBwbGllciIsIGNvbW1lbnQ9IlN1cHBsaWVyIGRpbWVuc2lvbiIpCmRlZiBkaW1fc3VwcGxpZXIoKToKICAgIHJldHVybiBzcGFyay5yZWFkLnRhYmxlKGYie3NpbHZlcn0uc2lsdmVyX3N1cHBsaWVycyIpLnNlbGVjdCgKICAgICAgICAic3VwcGxpZXJfaWQiLCAic3VwcGxpZXJfbmFtZSIsICJob21lX3JlZ2lvbiIsICJhY3RpdmUiKQoKCkBkcC5tYXRlcmlhbGl6ZWRfdmlldyhuYW1lPWYie2dvbGR9LmRpbV93YXJlaG91c2UiLCBjb21tZW50PSJEZXBvdCBkaW1lbnNpb24iKQpkZWYgZGltX3dhcmVob3VzZSgpOgogICAgcmV0dXJuIHNwYXJrLnJlYWQudGFibGUoZiJ7c2lsdmVyfS5zaWx2ZXJfd2FyZWhvdXNlcyIpLnNlbGVjdCgKICAgICAgICAid2FyZWhvdXNlX2lkIiwgIndhcmVob3VzZV9uYW1lIiwgImJvZHkiLCAicmVnaW9uIiwgInVwbGlua19yZWxpYWJpbGl0eSIpCgoKQGRwLm1hdGVyaWFsaXplZF92aWV3KG5hbWU9ZiJ7Z29sZH0uZGltX2NhdGVnb3J5IiwgY29tbWVudD0iQ2F0ZWdvcnkgZGltZW5zaW9uIikKZGVmIGRpbV9jYXRlZ29yeSgpOgogICAgcmV0dXJuIHNwYXJrLnJlYWQudGFibGUoZiJ7c2lsdmVyfS5zaWx2ZXJfY2F0ZWdvcmllcyIpLnNlbGVjdCgKICAgICAgICAiY2F0ZWdvcnlfaWQiLCAiY2F0ZWdvcnlfbmFtZSIsICJkZXBhcnRtZW50IikKCgpAZHAubWF0ZXJpYWxpemVkX3ZpZXcobmFtZT1mIntnb2xkfS5kaW1fZGF0ZSIsIGNvbW1lbnQ9IkdlbmVyYXRlZCBkYXRlIGRpbWVuc2lvbiIpCmRlZiBkaW1fZGF0ZSgpOgogICAgZGF0ZXMgPSAoCiAgICAgICAgc3BhcmsucmVhZC50YWJsZShmIntzaWx2ZXJ9LnNpbHZlcl9vcmRlcl9saW5lcyIpLnNlbGVjdCh0b19kYXRlKCJsaW5lX3RzIikuYWxpYXMoImQiKSkKICAgICAgICAudW5pb24oc3BhcmsucmVhZC50YWJsZShmIntzaWx2ZXJ9LnNpbHZlcl9pbnZlbnRvcnkiKS5zZWxlY3QodG9fZGF0ZSgibW92ZW1lbnRfdHMiKS5hbGlhcygiZCIpKSkKICAgICkKICAgIHNwYW4gPSBkYXRlcy5zZWxlY3QobWluXygiZCIpLmFsaWFzKCJmcm9tX2RhdGUiKSwgbWF4XygiZCIpLmFsaWFzKCJ0b19kYXRlIikpCiAgICByZXR1cm4gKAogICAgICAgIHNwYW4uc2VsZWN0KGV4cGxvZGUoc2VxdWVuY2UoY29sKCJmcm9tX2RhdGUiKSwgY29sKCJ0b19kYXRlIiksIGV4cHIoIklOVEVSVkFMIDEgREFZIikpKS5hbGlhcygiZGF0ZSIpKQogICAgICAgIC5zZWxlY3QoCiAgICAgICAgICAgIGRhdGVfZm9ybWF0KCJkYXRlIiwgInl5eXlNTWRkIikuY2FzdCgiaW50IikuYWxpYXMoImRhdGVfa2V5IiksIGNvbCgiZGF0ZSIpLAogICAgICAgICAgICB5ZWFyKCJkYXRlIikuYWxpYXMoInllYXIiKSwgbW9udGgoImRhdGUiKS5hbGlhcygibW9udGgiKSwgZGF5b2Ztb250aCgiZGF0ZSIpLmFsaWFzKCJkYXkiKSwKICAgICAgICAgICAgZGF0ZV9mb3JtYXQoImRhdGUiLCAiRUVFRSIpLmFsaWFzKCJkYXlfbmFtZSIpLCBkYXlvZndlZWsoImRhdGUiKS5pc2luKDEsIDcpLmFsaWFzKCJpc193ZWVrZW5kIiksCiAgICAgICAgKQogICAgKQoKCiMgZGltX2N1c3RvbWVyIGRlcml2ZXMgdGhlIGNvbnRyYWN0IGNvbHVtbnMgZnJvbSB0aGUgY3VzdG9tZXIgU0NEMiBzdHJlYW1pbmcgdGFibGUuIFRoZSBlbmdpbmUgZW1pdHMgX19TVEFSVF9BVAojIGFuZCBfX0VORF9BVCwgbm90IHRoZSBjb3Vyc2UncyBuYW1lcywgc28gaGVyZSB3ZSBtYXAgdGhlbTogZWZmZWN0aXZlX2Zyb20gaXMgX19TVEFSVF9BVCwgZWZmZWN0aXZlX3RvIGlzCiMgX19FTkRfQVQsIGlzX2N1cnJlbnQgaXMgX19FTkRfQVQgSVMgTlVMTCwgYW5kIHRoZSBzdXJyb2dhdGUga2V5IGlzIDxpZD5fPFlZWVlNTUREPi4gUHJpY2UgaXMgY29uZm9ybWVkIGluIFNpbHZlcgojIGFzIHNpbHZlcl9wcmljZV9zY2QsIHNvIHRoZXJlIGlzIG5vIEdvbGQgcHJpY2UgZGltZW5zaW9uOiB0aGUgZmFjdHMgcHJpY2UgcG9pbnQgaW4gdGltZSBzdHJhaWdodCBvZmYgU2lsdmVyLgpAZHAubWF0ZXJpYWxpemVkX3ZpZXcobmFtZT1mIntnb2xkfS5kaW1fY3VzdG9tZXIiLCBjb21tZW50PSJDdXN0b21lciBkaW1lbnNpb24sIFNDRCBUeXBlIDIgKHRpZXIgb3ZlciB0aW1lKSIpCmRlZiBkaW1fY3VzdG9tZXIoKToKICAgICMgVGhlIGN1c3RvbWVyIFNDRDIgdmVyc2lvbnMgYXJlIHNlcXVlbmNlZCBieSB0aGUgeXl5eU1NZGQgc25hcHNob3QgdmVyc2lvbiwgc28gX19TVEFSVF9BVC9fX0VORF9BVCBhcmUgdGhhdAogICAgIyBpbnRlZ2VyOyByZWFkIHRoZW0gYmFjayBhcyBkYXRlcy4KICAgIHJldHVybiAoCiAgICAgICAgc3BhcmsucmVhZC50YWJsZShmIntzaWx2ZXJ9LnNpbHZlcl9jdXN0b21lcl9zY2QiKQogICAgICAgIC5zZWxlY3QoCiAgICAgICAgICAgIGNvbmNhdF93cygiXyIsIGNvbCgiY3VzdG9tZXJfaWQiKSwgY29sKCJfX1NUQVJUX0FUIikuY2FzdCgic3RyaW5nIikpLmFsaWFzKCJjdXN0b21lcl9zayIpLAogICAgICAgICAgICAiY3VzdG9tZXJfaWQiLCAiY3VzdG9tZXJfbmFtZSIsICJjdXN0b21lcl90eXBlIiwgInRpZXIiLCAiaG9tZV93YXJlaG91c2VfaWQiLCAic2lnbnVwX2RhdGUiLAogICAgICAgICAgICB0b19kYXRlKGNvbCgiX19TVEFSVF9BVCIpLmNhc3QoInN0cmluZyIpLCAieXl5eU1NZGQiKS5hbGlhcygiZWZmZWN0aXZlX2Zyb20iKSwKICAgICAgICAgICAgdG9fZGF0ZShjb2woIl9fRU5EX0FUIikuY2FzdCgic3RyaW5nIiksICJ5eXl5TU1kZCIpLmFsaWFzKCJlZmZlY3RpdmVfdG8iKSwKICAgICAgICAgICAgY29sKCJfX0VORF9BVCIpLmlzTnVsbCgpLmFsaWFzKCJpc19jdXJyZW50IiksCiAgICAgICAgKQogICAgKQoKCiMgZmFjdF9vcmRlcl9saW5lczogbm93IHBvaW50IGluIHRpbWUgcHJpY2VkIG9uIHRoZSBwcmljZSBTQ0QyLiBFYWNoIGxpbmUgaXMgdmFsdWVkIGF0IHRoZSBwcmljZSB2ZXJzaW9uIHdob3NlCiMgZWZmZWN0aXZlIHJhbmdlIGNvdmVycyBpdHMgbGluZV90cyAoZWZmZWN0aXZlX2Zyb20gPD0gbGluZV90cyA8IGVmZmVjdGl2ZV90bywgYSBudWxsIGVmZmVjdGl2ZV90byBpcyBvcGVuKS4KIyBUaGUgcmFuZ2VzIGFyZSBub24gb3ZlcmxhcHBpbmcsIHNvIHRoZSBqb2luIHN0YXlzIG9uZSByb3cgcGVyIGxpbmUsIGFuZCBpc19yZXR1cm5lZCBmbGFncyB0aGUgbGluZXMgdGhhdCBjb21lCiMgYmFjay4gVGhlIGluY2lkZW50IGRheSBQUkQtMDA3IGxpbmVzIGF0IEFyZXMgdmFsdWUgYXQgMzQzNS4zMCBhbmQgY2FycnkgYSBuZWdhdGl2ZSBtYXJnaW4gKHNvbGQgYmVsb3cgY29zdCkuCiMgb3JkZXJfc3RhdHVzIGNhcnJpZXMgZWFjaCBsaW5lJ3Mgb3JkZXIgY3VycmVudCBsaWZlY3ljbGUgc3RhdHVzIChmcm9tIHNpbHZlcl9vcmRlcnMpIHNvIGNhbmNlbGxhdGlvbnMgY2FuIGJlCiMgbmV0dGVkIG91dCBkb3duc3RyZWFtOyBpdCBpcyBOVUxMIGZvciBwdXJnZWQgY2FuY2VsbGF0aW9ucywgd2hvc2UgbGluZXMgc3RpbGwgcmVtYWluIGluIHRoaXMgYm9va2luZ3MgZ3JhaW4uCkBkcC5tYXRlcmlhbGl6ZWRfdmlldyhuYW1lPWYie2dvbGR9LmZhY3Rfb3JkZXJfbGluZXMiLCBjbHVzdGVyX2J5PVsib3JkZXJfbGluZV9pZCJdLAogICAgICAgICAgICAgICAgICAgICAgY29tbWVudD0iT3JkZXIgbGluZSBmYWN0IChib29raW5ncyBncmFpbiwgb25lIHJvdyBwZXIgcGxhY2VkIGxpbmUpLCBwb2ludCBpbiB0aW1lIHByaWNlZCBvbiB0aGUgcHJpY2UgU0NEMiIpCmRlZiBmYWN0X29yZGVyX2xpbmVzKCk6CiAgICBsaW5lcyA9IHNwYXJrLnJlYWQudGFibGUoZiJ7c2lsdmVyfS5zaWx2ZXJfb3JkZXJfbGluZXMiKS5hbGlhcygibCIpCiAgICBwcmljZXMgPSBzcGFyay5yZWFkLnRhYmxlKGYie3NpbHZlcn0uc2lsdmVyX3ByaWNlX3NjZCIpLmFsaWFzKCJwIikKICAgIHJldHVybmVkID0gc3BhcmsucmVhZC50YWJsZShmIntzaWx2ZXJ9LnNpbHZlcl9yZXR1cm5zIikuc2VsZWN0KGNvbCgib3JkZXJfbGluZV9pZCIpLmFsaWFzKCJfcmV0X29sIikpLmRpc3RpbmN0KCkKICAgIG9yZGVycyA9IHNwYXJrLnJlYWQudGFibGUoZiJ7c2lsdmVyfS5zaWx2ZXJfb3JkZXJzIikuc2VsZWN0KGNvbCgib3JkZXJfaWQiKS5hbGlhcygiX29yZF9pZCIpLCBjb2woInN0YXR1cyIpLmFsaWFzKCJvcmRlcl9zdGF0dXMiKSkKICAgIHJldHVybiAoCiAgICAgICAgbGluZXMuam9pbigKICAgICAgICAgICAgcHJpY2VzLAogICAgICAgICAgICAoY29sKCJsLnByb2R1Y3RfaWQiKSA9PSBjb2woInAucHJvZHVjdF9pZCIpKQogICAgICAgICAgICAmICh0b19kYXRlKGNvbCgibC5saW5lX3RzIikpID49IGNvbCgicC5lZmZlY3RpdmVfZnJvbSIpKQogICAgICAgICAgICAmIChjb2woInAuZWZmZWN0aXZlX3RvIikuaXNOdWxsKCkgfCAodG9fZGF0ZShjb2woImwubGluZV90cyIpKSA8IGNvbCgicC5lZmZlY3RpdmVfdG8iKSkpLAogICAgICAgICAgICAibGVmdCIpCiAgICAgICAgLmpvaW4ocmV0dXJuZWQsIGNvbCgibC5vcmRlcl9saW5lX2lkIikgPT0gY29sKCJfcmV0X29sIiksICJsZWZ0IikKICAgICAgICAuam9pbihvcmRlcnMsIGNvbCgibC5vcmRlcl9pZCIpID09IGNvbCgiX29yZF9pZCIpLCAibGVmdCIpCiAgICAgICAgLnNlbGVjdCgKICAgICAgICAgICAgY29sKCJsLm9yZGVyX2xpbmVfaWQiKSwgY29sKCJsLm9yZGVyX2lkIiksIGNvbCgibC5wcm9kdWN0X2lkIiksIGNvbCgibC5jdXN0b21lcl9pZCIpLAogICAgICAgICAgICBjb2woImwud2FyZWhvdXNlX2lkIiksIGNvbCgicC5zdXBwbGllcl9pZCIpLAogICAgICAgICAgICBkYXRlX2Zvcm1hdChjb2woImwubGluZV90cyIpLCAieXl5eU1NZGQiKS5jYXN0KCJpbnQiKS5hbGlhcygiZGF0ZV9rZXkiKSwKICAgICAgICAgICAgY29sKCJwLnByaWNlX3NrIiksIGNvbCgibC5xdWFudGl0eSIpLCBjb2woInAudW5pdF9wcmljZSIpLCBjb2woInAudW5pdF9jb3N0IiksCiAgICAgICAgICAgIChjb2woImwucXVhbnRpdHkiKSAqIGNvbCgicC51bml0X3ByaWNlIikpLmNhc3QoImRlY2ltYWwoMTQsMikiKS5hbGlhcygiZ3Jvc3NfYW1vdW50IiksCiAgICAgICAgICAgIChjb2woImwucXVhbnRpdHkiKSAqIChjb2woInAudW5pdF9wcmljZSIpIC0gY29sKCJwLnVuaXRfY29zdCIpKSkuY2FzdCgiZGVjaW1hbCgxNCwyKSIpLmFsaWFzKCJtYXJnaW4iKSwKICAgICAgICAgICAgY29sKCJfcmV0X29sIikuaXNOb3ROdWxsKCkuYWxpYXMoImlzX3JldHVybmVkIiksCiAgICAgICAgICAgIGNvbCgib3JkZXJfc3RhdHVzIiksCiAgICAgICAgKQogICAgKQoKCiMgZmFjdF9yZXR1cm5zOiByZWZ1bmQgdmFsdWVkIGF0IHRoZSBPUklHSU5BTCBwYWlkIHByaWNlIChwb2ludCBpbiB0aW1lIGF0IHRoZSBsaW5lJ3MgbGluZV90cyksIGZyb20gYmF0Y2ggdHdvLgpAZHAubWF0ZXJpYWxpemVkX3ZpZXcobmFtZT1mIntnb2xkfS5mYWN0X3JldHVybnMiLCBjb21tZW50PSJSZXR1cm4gZmFjdCwgcmVmdW5kIGF0IHRoZSBvcmlnaW5hbCBwYWlkIHByaWNlIikKZGVmIGZhY3RfcmV0dXJucygpOgogICAgcmV0cyA9IHNwYXJrLnJlYWQudGFibGUoZiJ7c2lsdmVyfS5zaWx2ZXJfcmV0dXJucyIpLmFsaWFzKCJyIikKICAgIGxpbmVzID0gc3BhcmsucmVhZC50YWJsZShmIntzaWx2ZXJ9LnNpbHZlcl9vcmRlcl9saW5lcyIpLmFsaWFzKCJsIikKICAgIHByaWNlcyA9IHNwYXJrLnJlYWQudGFibGUoZiJ7c2lsdmVyfS5zaWx2ZXJfcHJpY2Vfc2NkIikuYWxpYXMoInAiKQogICAgcmV0dXJuICgKICAgICAgICByZXRzLmpvaW4obGluZXMsIGNvbCgici5vcmRlcl9saW5lX2lkIikgPT0gY29sKCJsLm9yZGVyX2xpbmVfaWQiKSwgImlubmVyIikKICAgICAgICAuam9pbihwcmljZXMsCiAgICAgICAgICAgICAgKGNvbCgibC5wcm9kdWN0X2lkIikgPT0gY29sKCJwLnByb2R1Y3RfaWQiKSkKICAgICAgICAgICAgICAmICh0b19kYXRlKGNvbCgibC5saW5lX3RzIikpID49IGNvbCgicC5lZmZlY3RpdmVfZnJvbSIpKQogICAgICAgICAgICAgICYgKGNvbCgicC5lZmZlY3RpdmVfdG8iKS5pc051bGwoKSB8ICh0b19kYXRlKGNvbCgibC5saW5lX3RzIikpIDwgY29sKCJwLmVmZmVjdGl2ZV90byIpKSksCiAgICAgICAgICAgICAgImxlZnQiKQogICAgICAgIC5zZWxlY3QoCiAgICAgICAgICAgIGNvbCgici5yZXR1cm5faWQiKSwgY29sKCJyLm9yZGVyX2xpbmVfaWQiKSwgY29sKCJsLnByb2R1Y3RfaWQiKSwgY29sKCJsLmN1c3RvbWVyX2lkIiksCiAgICAgICAgICAgIGNvbCgibC53YXJlaG91c2VfaWQiKSwKICAgICAgICAgICAgZGF0ZV9mb3JtYXQoY29sKCJyLnJldHVybl90cyIpLCAieXl5eU1NZGQiKS5jYXN0KCJpbnQiKS5hbGlhcygiZGF0ZV9rZXkiKSwKICAgICAgICAgICAgY29sKCJyLnF1YW50aXR5IikuYWxpYXMoInF1YW50aXR5X3JldHVybmVkIiksCiAgICAgICAgICAgIChjb2woInIucXVhbnRpdHkiKSAqIGNvbCgicC51bml0X3ByaWNlIikpLmNhc3QoImRlY2ltYWwoMTQsMikiKS5hbGlhcygicmVmdW5kX2Ftb3VudCIpLAogICAgICAgICkKICAgICkKCgojIGZhY3RfaW52ZW50b3J5OiBvbmUgcm93IHBlciBzdG9jayBtb3ZlbWVudCwgd2l0aCB0aGUgc2lnbmVkIGRlbHRhIGFuZCB0aGUgcnVubmluZyBvbl9oYW5kLgpAZHAubWF0ZXJpYWxpemVkX3ZpZXcobmFtZT1mIntnb2xkfS5mYWN0X2ludmVudG9yeSIsIGNvbW1lbnQ9IlN0b2NrIG1vdmVtZW50IGZhY3QiKQpkZWYgZmFjdF9pbnZlbnRvcnkoKToKICAgIHJldHVybiBzcGFyay5yZWFkLnRhYmxlKGYie3NpbHZlcn0uc2lsdmVyX2ludmVudG9yeSIpLnNlbGVjdCgKICAgICAgICAid2FyZWhvdXNlX2lkIiwgInByb2R1Y3RfaWQiLCAibW92ZW1lbnRfdHMiLAogICAgICAgIGRhdGVfZm9ybWF0KCJtb3ZlbWVudF90cyIsICJ5eXl5TU1kZCIpLmNhc3QoImludCIpLmFsaWFzKCJkYXRlX2tleSIpLCAiZGVsdGEiLCAib25faGFuZCIp";
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
# MAGIC Build and validate the gold facts: one row per key, every line priced point in time, the canonical line valued correctly, and fact_returns.

# COMMAND ----------

print("Pipeline run:", run_pipeline())

silver = f"{catalog}.helios_silver"
gold = f"{catalog}.helios_gold"
lines = spark.table(f"{silver}.silver_order_lines")
fol = spark.table(f"{gold}.fact_order_lines")
check("fact_order_lines is one row per line (no fan out)",
      fol.count(), fol.select("order_line_id").distinct().count())
check("fact_order_lines matches the clean silver lines", lines.count(), fol.count())
check("every line is priced by the point in time join", 0, fol.filter("price_sk IS NULL").count())
ol = fol.filter("order_line_id = 'OL-1-000001'").first()
check("OL-1-000001 is priced point in time at 10184.99", "10184.99", str(ol["unit_price"]))
check("OL-1-000001 uses the 2257-03-01 price version", "PRD-017_22570301", ol["price_sk"])
check("order_status is null or a valid lifecycle status", 0,
      fol.filter("order_status IS NOT NULL AND order_status NOT IN "
                 "('PLACED','PAID','PICKED','SHIPPED','DELIVERED','BACKORDERED','CANCELLED','RETURNED')").count())

fr = spark.table(f"{gold}.fact_returns")
check("fact_returns is one row per return", fr.count(), fr.select("return_id").distinct().count())
check("every return has a refund amount", 0, fr.filter("refund_amount IS NULL").count())
check_true("the conformed and SCD Type 2 dimensions exist",
           all(spark.catalog.tableExists(f"{gold}.{t}") for t in
               ["dim_product", "dim_supplier", "dim_warehouse", "dim_category", "dim_date",
                "dim_customer"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What you built
# MAGIC
# MAGIC The **gold** layer of the Helios medallion, declared in Lakeflow and built by the engine. You wrote the declarations, ran the one pipeline, and validated this layer from here.