# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 4.6: Build the quality gated silver layer
# MAGIC
# MAGIC ### Ticket: HELIOS-406
# MAGIC
# MAGIC **Context.** Section 4 rebuilds the Helios medallion declaratively with Lakeflow Spark Declarative Pipelines, one layer per lab, the same split as the Section 3 job tasks. In this lab you build the **silver** layer. The earlier layer (bronze) is provided complete in the source folder, so the pipeline has its inputs. You author the declarations in the source notebook `4_6_silver` (in the `4_6_pipeline_source` folder), then run the one pipeline from here and check the result.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bootstrap
# MAGIC Resets the layers the pipeline owns and lands batches one to three, then creates the one pipeline pointing at this lab's source notebooks (4_6_bronze, 4_6_silver). Gold lands in `helios_gold`, the one canonical Gold schema, the same target the Section 3 Jobs build writes. Run this top to bottom before the task.

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
src_dir = f"{_dir}/4_6_pipeline_source"
source_notebooks = ["4_6_bronze", "4_6_silver"]
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
# MAGIC Open `4_6_silver` in the `4_6_pipeline_source` folder. It opens with a provided scaffolding cell (do not edit) that imports the pipeline API and reads your catalog from the `helios.catalog` value this driver passes into the pipeline configuration, so you write only the table declarations in its `TODO` cell. Then run the check cell below: it triggers a pipeline run and validates this layer. A run takes a couple of minutes on serverless. The reveal has the solution if you get stuck.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your task: Build the quality gated silver layer
# MAGIC
# MAGIC In `4_6_silver`, build the silver tables that need no change tracking, and gate their quality with expectations,
# MAGIC quarantining the bad rows rather than dropping them silently. The target column names and types for every
# MAGIC table are in `4_3_pipeline_design.md`, in this folder, under Silver schemas.
# MAGIC
# MAGIC **How.**
# MAGIC - `silver_order_lines`: stream from bronze, type every column, set a 48 hour watermark on `line_ts` and
# MAGIC   deduplicate within it. Add three expectations that teach the three severities: `expect_or_fail` on the primary
# MAGIC   key (halts the pipeline if it is ever null), and `expect_or_drop` on `quantity > 0` and `product_id IS NOT
# MAGIC   NULL` (drop the contract breaking rows). A second flow, `silver_order_lines_quarantine`, keeps exactly those
# MAGIC   dropped rows so a data quality problem stays visible.
# MAGIC - `silver_inventory` and the four reference dimensions (`silver_products`, `silver_categories`,
# MAGIC   `silver_suppliers`, `silver_warehouses`): typed, the dims as materialized views.
# MAGIC - `silver_returns`: a return is valid only if it points at a real order line and is dated after it, a referential
# MAGIC   rule that needs a join, so split clean and quarantine by a filter on the join; keep the per row rules as
# MAGIC   expectations (`expect_or_fail` on `return_id`, a warn `expect` on the reason enum).
# MAGIC
# MAGIC **Definition of done.**
# MAGIC - `silver_order_lines` has no duplicate `order_line_id` (its row count equals its distinct `order_line_id` count).
# MAGIC - No dirty row survives in `silver_order_lines`: zero rows match `quantity IS NULL OR quantity <= 0 OR product_id IS NULL`.
# MAGIC - `silver_order_lines_quarantine` holds only dirty rows (zero rows fail that same rule), and it captured some (its count is greater than 0).
# MAGIC - The event log drop count, the summed `failed_records` for the `positive_quantity` and `product_present` expectations on the latest update, equals the `silver_order_lines_quarantine` row count.
# MAGIC - `silver_returns` points only at real lines: every row joins to an `order_line_id` in `silver_order_lines` (a left anti join returns 0 rows).
# MAGIC - `silver_returns_quarantine` is built for the orphan or early returns (no matching line, or dated on or before their line).
# MAGIC - The typed reference dimensions all exist as tables: `silver_products`, `silver_categories`, `silver_suppliers`, `silver_warehouses`.
# MAGIC - `silver_inventory` exists as a typed stock movement table.
# MAGIC
# MAGIC **References.** You may find these Databricks docs helpful:
# MAGIC - [Manage data quality with pipeline expectations](https://docs.databricks.com/aws/en/dlt/expectations)
# MAGIC - [Streaming tables](https://docs.databricks.com/aws/en/dlt/streaming-tables)
# MAGIC - [Pipeline event log](https://docs.databricks.com/aws/en/ldp/monitor-event-logs)
# MAGIC - [Materialized views](https://docs.databricks.com/aws/en/dlt/materialized-views)

# COMMAND ----------

# Your turn. Write the silver declarations in 4_6_pipeline_source/4_6_silver, then run the check cell.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Build the quality gated silver layer (solution)
# MAGIC <details>
# MAGIC <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; overflow:auto;"></pre>
# MAGIC
# MAGIC <script>
# MAGIC var codeB64 = "IyBzaWx2ZXJfb3JkZXJfbGluZXM6IHR5cGVkLCBkZWR1cGxpY2F0ZWQsIGFuZCBub3cgcXVhbGl0eSBnYXRlZC4gVGhyZWUgZXhwZWN0YXRpb25zIHRlYWNoIHRoZSB0aHJlZSBzZXZlcml0aWVzOgojICAgZXhwZWN0X29yX2ZhaWwgIGhhbHRzIHRoZSBwaXBlbGluZSBpZiB0aGUgcHJpbWFyeSBrZXkgaXMgZXZlciBudWxsIChpdCBuZXZlciBpcywgYnkgY29udHJhY3QpLAojICAgZXhwZWN0X29yX2Ryb3AgIHJlbW92ZXMgdGhlIGNvbnRyYWN0IGJyZWFraW5nIHJvd3MgKGEgbm9uIHBvc2l0aXZlIHF1YW50aXR5LCBvciBhIG1pc3NpbmcgcHJvZHVjdCksCiMgYW5kIHRoZSBkcm9wcGVkIHJvd3MgYXJlIG5vdCBsb3N0OiBhIHNlY29uZCBmbG93IHdyaXRlcyB0aGVtIHRvIHNpbHZlcl9vcmRlcl9saW5lc19xdWFyYW50aW5lIHRvIGludmVzdGlnYXRlLgpAZHAudGFibGUobmFtZT1mIntzaWx2ZXJ9LnNpbHZlcl9vcmRlcl9saW5lcyIsIGNsdXN0ZXJfYnk9WyJvcmRlcl9saW5lX2lkIl0sCiAgICAgICAgICBjb21tZW50PSJUeXBlZCwgZGVkdXBsaWNhdGVkLCBxdWFsaXR5IGdhdGVkIG9yZGVyIGxpbmVzIChvbmUgY2xlYW4gcm93IHBlciBvcmRlcl9saW5lX2lkKSIpCkBkcC5leHBlY3Rfb3JfZmFpbCgib3JkZXJfbGluZV9pZF9wcmVzZW50IiwgIm9yZGVyX2xpbmVfaWQgSVMgTk9UIE5VTEwiKQpAZHAuZXhwZWN0X29yX2Ryb3AoInBvc2l0aXZlX3F1YW50aXR5IiwgInF1YW50aXR5ID4gMCIpCkBkcC5leHBlY3Rfb3JfZHJvcCgicHJvZHVjdF9wcmVzZW50IiwgInByb2R1Y3RfaWQgSVMgTk9UIE5VTEwiKQpkZWYgc2lsdmVyX29yZGVyX2xpbmVzKCk6CiAgICByZXR1cm4gX3R5cGVkX29yZGVyX2xpbmVzKCkKCgpAZHAudGFibGUobmFtZT1mIntzaWx2ZXJ9LnNpbHZlcl9vcmRlcl9saW5lc19xdWFyYW50aW5lIiwKICAgICAgICAgIGNvbW1lbnQ9Ik9yZGVyIGxpbmVzIHRoYXQgYnJva2UgdGhlIHF1YW50aXR5IG9yIHByb2R1Y3QgcnVsZSwga2VwdCBmb3IgaW52ZXN0aWdhdGlvbiIpCmRlZiBzaWx2ZXJfb3JkZXJfbGluZXNfcXVhcmFudGluZSgpOgogICAgcmV0dXJuIF90eXBlZF9vcmRlcl9saW5lcygpLmZpbHRlcigicXVhbnRpdHkgSVMgTlVMTCBPUiBxdWFudGl0eSA8PSAwIE9SIHByb2R1Y3RfaWQgSVMgTlVMTCIpCgoKZGVmIF90eXBlZF9vcmRlcl9saW5lcygpOgogICAgIyBUeXBlIGV2ZXJ5IGNvbHVtbiwgdGhlbiBkZWR1cGxpY2F0ZSB0byBvbmUgcm93IHBlciBvcmRlcl9saW5lX2lkIHdpdGhpbiBhIDQ4IGhvdXIgd2F0ZXJtYXJrLiBUaGUgd2F0ZXJtYXJrCiAgICAjIHJlbW92ZXMgdGhlIGF0IGxlYXN0IG9uY2UgZHVwbGljYXRlIHN1Ym1pc3Npb25zIGFuZCBib3VuZHMgdGhlIHN0cmVhbWluZyBzdGF0ZSBmb3IgbGF0ZSBhcnJpdmFscy4KICAgIHJldHVybiAoCiAgICAgICAgc3BhcmsucmVhZFN0cmVhbS50YWJsZShmInticm9uemV9LmJyb256ZV9vcmRlcl9saW5lcyIpCiAgICAgICAgLnNlbGVjdCgKICAgICAgICAgICAgY29sKCJvcmRlcl9saW5lX2lkIikuY2FzdCgic3RyaW5nIikuYWxpYXMoIm9yZGVyX2xpbmVfaWQiKSwKICAgICAgICAgICAgY29sKCJvcmRlcl9pZCIpLmNhc3QoInN0cmluZyIpLmFsaWFzKCJvcmRlcl9pZCIpLAogICAgICAgICAgICBjb2woInByb2R1Y3RfaWQiKS5jYXN0KCJzdHJpbmciKS5hbGlhcygicHJvZHVjdF9pZCIpLAogICAgICAgICAgICBjb2woImN1c3RvbWVyX2lkIikuY2FzdCgic3RyaW5nIikuYWxpYXMoImN1c3RvbWVyX2lkIiksCiAgICAgICAgICAgIGNvbCgid2FyZWhvdXNlX2lkIikuY2FzdCgic3RyaW5nIikuYWxpYXMoIndhcmVob3VzZV9pZCIpLAogICAgICAgICAgICBjb2woInF1YW50aXR5IikuY2FzdCgiaW50IikuYWxpYXMoInF1YW50aXR5IiksCiAgICAgICAgICAgIGNvbCgibGluZV90cyIpLmNhc3QoInRpbWVzdGFtcCIpLmFsaWFzKCJsaW5lX3RzIiksCiAgICAgICAgKQogICAgICAgIC53aXRoV2F0ZXJtYXJrKCJsaW5lX3RzIiwgIjQ4IGhvdXJzIikKICAgICAgICAuZHJvcER1cGxpY2F0ZXNXaXRoaW5XYXRlcm1hcmsoWyJvcmRlcl9saW5lX2lkIl0pCiAgICApCgoKIyBzaWx2ZXJfaW52ZW50b3J5IGFuZCB0aGUgZm91ciByZWZlcmVuY2UgZGltZW5zaW9uczogdHlwZWQsIG9uZSByb3cgcGVyIGtleS4KQGRwLnRhYmxlKG5hbWU9ZiJ7c2lsdmVyfS5zaWx2ZXJfaW52ZW50b3J5IiwgY29tbWVudD0iVHlwZWQgc3RvY2sgbW92ZW1lbnQgbG9nIikKZGVmIHNpbHZlcl9pbnZlbnRvcnkoKToKICAgIHJldHVybiAoCiAgICAgICAgc3BhcmsucmVhZFN0cmVhbS50YWJsZShmInticm9uemV9LmJyb256ZV9pbnZlbnRvcnkiKQogICAgICAgIC5zZWxlY3QoCiAgICAgICAgICAgIGNvbCgid2FyZWhvdXNlX2lkIikuY2FzdCgic3RyaW5nIikuYWxpYXMoIndhcmVob3VzZV9pZCIpLAogICAgICAgICAgICBjb2woInByb2R1Y3RfaWQiKS5jYXN0KCJzdHJpbmciKS5hbGlhcygicHJvZHVjdF9pZCIpLAogICAgICAgICAgICBjb2woIm1vdmVtZW50X3RzIikuY2FzdCgidGltZXN0YW1wIikuYWxpYXMoIm1vdmVtZW50X3RzIiksCiAgICAgICAgICAgIGNvbCgiZGVsdGEiKS5jYXN0KCJpbnQiKS5hbGlhcygiZGVsdGEiKSwKICAgICAgICAgICAgY29sKCJvbl9oYW5kIikuY2FzdCgiaW50IikuYWxpYXMoIm9uX2hhbmQiKSwKICAgICAgICApCiAgICApCgoKQGRwLm1hdGVyaWFsaXplZF92aWV3KG5hbWU9ZiJ7c2lsdmVyfS5zaWx2ZXJfcHJvZHVjdHMiLCBjb21tZW50PSJUeXBlZCBwcm9kdWN0IHJlZmVyZW5jZSIpCmRlZiBzaWx2ZXJfcHJvZHVjdHMoKToKICAgIHJldHVybiAoCiAgICAgICAgc3BhcmsucmVhZC50YWJsZShmInticm9uemV9LmJyb256ZV9wcm9kdWN0cyIpCiAgICAgICAgLnNlbGVjdCgKICAgICAgICAgICAgY29sKCJwcm9kdWN0X2lkIikuY2FzdCgic3RyaW5nIikuYWxpYXMoInByb2R1Y3RfaWQiKSwKICAgICAgICAgICAgY29sKCJza3UiKS5jYXN0KCJzdHJpbmciKS5hbGlhcygic2t1IiksCiAgICAgICAgICAgIGNvbCgicHJvZHVjdF9uYW1lIikuY2FzdCgic3RyaW5nIikuYWxpYXMoInByb2R1Y3RfbmFtZSIpLAogICAgICAgICAgICBjb2woImNhdGVnb3J5X2lkIikuY2FzdCgic3RyaW5nIikuYWxpYXMoImNhdGVnb3J5X2lkIiksCiAgICAgICAgICAgIGNvbCgic3VwcGxpZXJfaWQiKS5jYXN0KCJzdHJpbmciKS5hbGlhcygic3VwcGxpZXJfaWQiKSwKICAgICAgICAgICAgY29sKCJtYXNzX2tnIikuY2FzdCgiZG91YmxlIikuYWxpYXMoIm1hc3Nfa2ciKSwKICAgICAgICAgICAgY29sKCJoYXphcmRfY2xhc3MiKS5jYXN0KCJzdHJpbmciKS5hbGlhcygiaGF6YXJkX2NsYXNzIiksCiAgICAgICAgICAgIGNvbCgiYWN0aXZlIikuY2FzdCgiYm9vbGVhbiIpLmFsaWFzKCJhY3RpdmUiKSwKICAgICAgICApCiAgICAgICAgLmRyb3BEdXBsaWNhdGVzKFsicHJvZHVjdF9pZCJdKQogICAgKQoKCkBkcC5tYXRlcmlhbGl6ZWRfdmlldyhuYW1lPWYie3NpbHZlcn0uc2lsdmVyX2NhdGVnb3JpZXMiLCBjb21tZW50PSJUeXBlZCBjYXRlZ29yeSByZWZlcmVuY2UiKQpkZWYgc2lsdmVyX2NhdGVnb3JpZXMoKToKICAgIHJldHVybiAoCiAgICAgICAgc3BhcmsucmVhZC50YWJsZShmInticm9uemV9LmJyb256ZV9jYXRlZ29yaWVzIikKICAgICAgICAuc2VsZWN0KAogICAgICAgICAgICBjb2woImNhdGVnb3J5X2lkIikuY2FzdCgic3RyaW5nIikuYWxpYXMoImNhdGVnb3J5X2lkIiksCiAgICAgICAgICAgIGNvbCgiY2F0ZWdvcnlfbmFtZSIpLmNhc3QoInN0cmluZyIpLmFsaWFzKCJjYXRlZ29yeV9uYW1lIiksCiAgICAgICAgICAgIGNvbCgiZGVwYXJ0bWVudCIpLmNhc3QoInN0cmluZyIpLmFsaWFzKCJkZXBhcnRtZW50IiksCiAgICAgICAgKQogICAgICAgIC5kcm9wRHVwbGljYXRlcyhbImNhdGVnb3J5X2lkIl0pCiAgICApCgoKQGRwLm1hdGVyaWFsaXplZF92aWV3KG5hbWU9ZiJ7c2lsdmVyfS5zaWx2ZXJfc3VwcGxpZXJzIiwgY29tbWVudD0iVHlwZWQgc3VwcGxpZXIgcmVmZXJlbmNlIikKZGVmIHNpbHZlcl9zdXBwbGllcnMoKToKICAgIHJldHVybiAoCiAgICAgICAgc3BhcmsucmVhZC50YWJsZShmInticm9uemV9LmJyb256ZV9zdXBwbGllcnMiKQogICAgICAgIC5zZWxlY3QoCiAgICAgICAgICAgIGNvbCgic3VwcGxpZXJfaWQiKS5jYXN0KCJzdHJpbmciKS5hbGlhcygic3VwcGxpZXJfaWQiKSwKICAgICAgICAgICAgY29sKCJzdXBwbGllcl9uYW1lIikuY2FzdCgic3RyaW5nIikuYWxpYXMoInN1cHBsaWVyX25hbWUiKSwKICAgICAgICAgICAgY29sKCJob21lX3JlZ2lvbiIpLmNhc3QoInN0cmluZyIpLmFsaWFzKCJob21lX3JlZ2lvbiIpLAogICAgICAgICAgICBjb2woImFjdGl2ZSIpLmNhc3QoImJvb2xlYW4iKS5hbGlhcygiYWN0aXZlIiksCiAgICAgICAgKQogICAgICAgIC5kcm9wRHVwbGljYXRlcyhbInN1cHBsaWVyX2lkIl0pCiAgICApCgoKQGRwLm1hdGVyaWFsaXplZF92aWV3KG5hbWU9ZiJ7c2lsdmVyfS5zaWx2ZXJfd2FyZWhvdXNlcyIsIGNvbW1lbnQ9IlR5cGVkIGRlcG90IHJlZmVyZW5jZSIpCmRlZiBzaWx2ZXJfd2FyZWhvdXNlcygpOgogICAgcmV0dXJuICgKICAgICAgICBzcGFyay5yZWFkLnRhYmxlKGYie2Jyb256ZX0uYnJvbnplX3dhcmVob3VzZXMiKQogICAgICAgIC5zZWxlY3QoCiAgICAgICAgICAgIGNvbCgid2FyZWhvdXNlX2lkIikuY2FzdCgic3RyaW5nIikuYWxpYXMoIndhcmVob3VzZV9pZCIpLAogICAgICAgICAgICBjb2woIndhcmVob3VzZV9uYW1lIikuY2FzdCgic3RyaW5nIikuYWxpYXMoIndhcmVob3VzZV9uYW1lIiksCiAgICAgICAgICAgIGNvbCgiYm9keSIpLmNhc3QoInN0cmluZyIpLmFsaWFzKCJib2R5IiksCiAgICAgICAgICAgIGNvbCgicmVnaW9uIikuY2FzdCgic3RyaW5nIikuYWxpYXMoInJlZ2lvbiIpLAogICAgICAgICAgICBjb2woInVwbGlua19yZWxpYWJpbGl0eSIpLmNhc3QoImRvdWJsZSIpLmFsaWFzKCJ1cGxpbmtfcmVsaWFiaWxpdHkiKSwKICAgICAgICApCiAgICAgICAgLmRyb3BEdXBsaWNhdGVzKFsid2FyZWhvdXNlX2lkIl0pCiAgICApCgoKIyBzaWx2ZXJfcmV0dXJuczogcmV0dXJucyBsYW5kIGZyb20gYmF0Y2ggdHdvLiBBIHJldHVybiBpcyB2YWxpZCBvbmx5IGlmIGl0IHBvaW50cyBhdCBhIHJlYWwgb3JkZXIgbGluZSBhbmQgaXMKIyBkYXRlZCBhZnRlciBpdCwgd2hpY2ggaXMgYSByZWZlcmVudGlhbCBydWxlIHRoYXQgbmVlZHMgYSBqb2luLCBzbyB0aGUgY2xlYW4gYW5kIHF1YXJhbnRpbmUgc3BsaXQgaXMgZG9uZSBieSBhCiMgZmlsdGVyIG9uIHRoZSBqb2luLiBUaGUgcGVyIHJvdyBkb21haW4gcnVsZXMgYXJlIGV4cHJlc3Npb25zOiBleHBlY3Rfb3JfZmFpbCBvbiB0aGUgcHJpbWFyeSBrZXksIGFuZCBhIHdhcm4KIyBleHBlY3RhdGlvbiBvbiB0aGUgcmVhc29uIGVudW0gKHdhcm4gd3JpdGVzIGV2ZXJ5IHJvdyBhbmQgZmxhZ3MgdmlvbGF0aW9ucyBpbiB0aGUgZXZlbnQgbG9nLCBpdCBkb2VzIG5vdCBkcm9wKS4KZGVmIF9yZXR1cm5zX2pvaW5lZCgpOgogICAgcmV0dXJucyA9ICgKICAgICAgICBzcGFyay5yZWFkU3RyZWFtLnRhYmxlKGYie2Jyb256ZX0uYnJvbnplX3JldHVybnMiKQogICAgICAgIC5zZWxlY3QoCiAgICAgICAgICAgIGNvbCgicmV0dXJuX2lkIikuY2FzdCgic3RyaW5nIikuYWxpYXMoInJldHVybl9pZCIpLAogICAgICAgICAgICBjb2woIm9yZGVyX2xpbmVfaWQiKS5jYXN0KCJzdHJpbmciKS5hbGlhcygib3JkZXJfbGluZV9pZCIpLAogICAgICAgICAgICBjb2woInJldHVybl90cyIpLmNhc3QoInRpbWVzdGFtcCIpLmFsaWFzKCJyZXR1cm5fdHMiKSwKICAgICAgICAgICAgY29sKCJxdWFudGl0eSIpLmNhc3QoImludCIpLmFsaWFzKCJxdWFudGl0eSIpLAogICAgICAgICAgICBjb2woInJlYXNvbiIpLmNhc3QoInN0cmluZyIpLmFsaWFzKCJyZWFzb24iKSwKICAgICAgICApCiAgICApCiAgICBsaW5lcyA9IHNwYXJrLnJlYWQudGFibGUoZiJ7c2lsdmVyfS5zaWx2ZXJfb3JkZXJfbGluZXMiKS5zZWxlY3QoCiAgICAgICAgY29sKCJvcmRlcl9saW5lX2lkIikuYWxpYXMoIl9vbCIpLCBjb2woImxpbmVfdHMiKS5hbGlhcygiX2xpbmVfdHMiKSkKICAgIHJldHVybiByZXR1cm5zLmpvaW4obGluZXMsIGNvbCgib3JkZXJfbGluZV9pZCIpID09IGNvbCgiX29sIiksICJsZWZ0IikKCgpAZHAudGFibGUobmFtZT1mIntzaWx2ZXJ9LnNpbHZlcl9yZXR1cm5zIiwgY29tbWVudD0iVHlwZWQgcmV0dXJucyB0aGF0IHBvaW50IGF0IGEgcmVhbCBsaW5lIGFuZCBhcmUgZGF0ZWQgYWZ0ZXIgaXQiKQpAZHAuZXhwZWN0X29yX2ZhaWwoInJldHVybl9pZF9wcmVzZW50IiwgInJldHVybl9pZCBJUyBOT1QgTlVMTCIpCkBkcC5leHBlY3QoInZhbGlkX3JlYXNvbiIsIGYicmVhc29uIElOIHtSRVRVUk5fUkVBU09OU30iKQpkZWYgc2lsdmVyX3JldHVybnMoKToKICAgIGNvbHMgPSBbInJldHVybl9pZCIsICJvcmRlcl9saW5lX2lkIiwgInJldHVybl90cyIsICJxdWFudGl0eSIsICJyZWFzb24iXQogICAgcmV0dXJuIF9yZXR1cm5zX2pvaW5lZCgpLmZpbHRlcihjb2woIl9saW5lX3RzIikuaXNOb3ROdWxsKCkgJiAoY29sKCJyZXR1cm5fdHMiKSA+IGNvbCgiX2xpbmVfdHMiKSkpLnNlbGVjdCgqY29scykKCgpAZHAudGFibGUobmFtZT1mIntzaWx2ZXJ9LnNpbHZlcl9yZXR1cm5zX3F1YXJhbnRpbmUiLAogICAgICAgICAgY29tbWVudD0iT3JwaGFuIHJldHVybnMgKG5vIG1hdGNoaW5nIGxpbmUpIG9yIHJldHVybnMgZGF0ZWQgb24gb3IgYmVmb3JlIHRoZWlyIGxpbmUiKQpkZWYgc2lsdmVyX3JldHVybnNfcXVhcmFudGluZSgpOgogICAgY29scyA9IFsicmV0dXJuX2lkIiwgIm9yZGVyX2xpbmVfaWQiLCAicmV0dXJuX3RzIiwgInF1YW50aXR5IiwgInJlYXNvbiJdCiAgICByZXR1cm4gX3JldHVybnNfam9pbmVkKCkuZmlsdGVyKGNvbCgiX2xpbmVfdHMiKS5pc051bGwoKSB8IChjb2woInJldHVybl90cyIpIDw9IGNvbCgiX2xpbmVfdHMiKSkpLnNlbGVjdCgqY29scyk=";
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
# MAGIC Confirms the dedup, the quality-gate drop==quarantine via the event log, and that returns are referential.

# COMMAND ----------

print("Pipeline run:", run_pipeline())

from pyspark.sql.functions import explode, from_json, expr, sum as sum_
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType
silver = f"{catalog}.helios_silver"
lines = spark.table(f"{silver}.silver_order_lines")
check("silver_order_lines has no duplicate order_line_id",
      lines.count(), lines.select("order_line_id").distinct().count())
rule = "quantity IS NULL OR quantity <= 0 OR product_id IS NULL"
check("no dirty rows survive in silver_order_lines", 0, lines.filter(rule).count())
q = spark.table(f"{silver}.silver_order_lines_quarantine")
check("the quarantine holds only dirty rows", 0, q.filter(f"NOT ({rule})").count())
check_true("the quarantine captured some dirty rows", q.count() > 0)

# Event log: the two drop expectations dropped exactly the rows now in the quarantine table.
pid = find_pipeline().pipeline_id
events = spark.sql(f"SELECT * FROM event_log('{pid}')")
latest = (events.filter("event_type = 'create_update'")
          .orderBy(col("timestamp").desc()).select(col("origin.update_id").alias("uid")).first())
uid = latest["uid"] if latest else None
exp_schema = ArrayType(StructType([
    StructField("name", StringType()),
    StructField("dataset", StringType()),
    StructField("passed_records", IntegerType()),
    StructField("failed_records", IntegerType()),
]))
dropped = (events.filter((col("event_type") == "flow_progress") & (col("origin.update_id") == uid))
           .select(explode(from_json(expr("details:flow_progress:data_quality:expectations"), exp_schema)).alias("e"))
           .filter("e.name IN ('positive_quantity','product_present')")
           .agg(sum_("e.failed_records").alias("f")).first()["f"]) or 0
check("the event log drop count equals the quarantine table count", q.count(), int(dropped))

returns = spark.table(f"{silver}.silver_returns")
check("every silver return points at a real order line", 0,
      returns.select("order_line_id").join(lines.select("order_line_id"), "order_line_id", "left_anti").count())
check_true("the reference dimensions and inventory exist",
           all(spark.catalog.tableExists(f"{silver}.{t}") for t in
               ["silver_products", "silver_categories", "silver_suppliers", "silver_warehouses", "silver_inventory"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What you built
# MAGIC
# MAGIC The **silver** layer of the Helios medallion, declared in Lakeflow and built by the engine. You wrote the declarations, ran the one pipeline, and validated this layer from here.