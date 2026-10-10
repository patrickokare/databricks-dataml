# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 4.7: Reduce the change feeds with AUTO CDC and SCD Type 2
# MAGIC
# MAGIC ### Ticket: HELIOS-407
# MAGIC
# MAGIC **Context.** Section 4 rebuilds the Helios medallion declaratively with Lakeflow Spark Declarative Pipelines, one layer per lab, the same split as the Section 3 job tasks. In this lab you build the **scd** layer. The earlier layers (bronze, silver) are provided complete in the source folder, so the pipeline has its inputs. You author the declarations in the source notebook `4_7_scd` (in the `4_7_pipeline_source` folder), then run the one pipeline from here and check the result.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bootstrap
# MAGIC Resets the layers the pipeline owns and lands batches one to three, then creates the one pipeline pointing at this lab's source notebooks (4_7_bronze, 4_7_silver, 4_7_scd). Gold lands in `helios_gold`, the one canonical Gold schema, the same target the Section 3 Jobs build writes. Run this top to bottom before the task.

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
src_dir = f"{_dir}/4_7_pipeline_source"
source_notebooks = ["4_7_bronze", "4_7_silver", "4_7_scd"]
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
# MAGIC Open `4_7_scd` in the `4_7_pipeline_source` folder. It opens with a provided scaffolding cell (do not edit) that imports the pipeline API and reads your catalog from the `helios.catalog` value this driver passes into the pipeline configuration, so you write only the table declarations in its `TODO` cell. Then run the check cell below: it triggers a pipeline run and validates this layer. A run takes a couple of minutes on serverless. The reveal has the solution if you get stuck.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your task: Reduce the change feeds with AUTO CDC and SCD Type 2
# MAGIC
# MAGIC In `4_7_scd`, reduce the orders change feed to current state and build the price and customer histories as SCD
# MAGIC Type 2, all with AUTO CDC. One directive each replaces the entire foreachBatch MERGE from Lab 3.8 and the window
# MAGIC plus MERGE from Lab 3.9.
# MAGIC
# MAGIC The target column names and types for every table are in `4_3_pipeline_design.md`, in this folder.
# MAGIC
# MAGIC **How.**
# MAGIC - `silver_orders`: type the change feed in a pipeline scoped view, then `create_streaming_table` and
# MAGIC   `create_auto_cdc_flow` keyed on `order_id`, sequenced by `change_seq` (the highest is current, because
# MAGIC   `change_ts` can arrive out of order), with `apply_as_deletes` for a CDC `op` of DELETE and `except_column_list`
# MAGIC   dropping `op`. The default is SCD type 1 (current state), which is what an order's status needs.
# MAGIC - `silver_price_scd`: the price feed carries explicit version dates, so build the raw versions with
# MAGIC   `create_auto_cdc_flow` keyed `product_id`, sequenced by `effective_from`, `stored_as_scd_type=2` into
# MAGIC   `silver_price_versions` (the engine adds `__START_AT`/`__END_AT`), then a materialized view `silver_price_scd`
# MAGIC   maps those to the contract columns (`effective_from`/`effective_to`, `is_current`) and builds the `price_sk`
# MAGIC   surrogate key, so Gold prices point in time straight off Silver with no separate price dimension.
# MAGIC - `silver_customer_scd`: the customers feed is a bare daily snapshot with no version dates, so it uses
# MAGIC   `create_auto_cdc_from_snapshot_flow`: a function hands the engine each day's snapshot in order (the date as a
# MAGIC   `yyyyMMdd` integer version), and the engine opens a new version when a customer's tier changes. This contrast
# MAGIC   with the price feed is the teaching point.
# MAGIC
# MAGIC **Example.** `PRD-007` (the Ares incident part) lands two price versions by batch three: `7633.99` effective
# MAGIC `2257-03-01`, then the below cost mispricing `3435.30` effective `2257-03-03`. `CUST-0033` is DRIFTER in the first
# MAGIC two snapshots and TRADE in the batch three snapshot, so it ends with two tier versions.
# MAGIC
# MAGIC **Definition of done.**
# MAGIC - `silver_orders` holds one row per `order_id` (its row count equals its distinct `order_id` count), reduced from the CDC feed to current state.
# MAGIC - `silver_orders` has no `op` column (the CDC flag is dropped by `except_column_list`).
# MAGIC - `ORD-000001` in `silver_orders` carries the same `status` as its latest change in `bronze_orders` (the row with the highest `change_seq`).
# MAGIC - No order whose latest change has `op = 'DELETE'` survives in `silver_orders`: it is soft deleted away.
# MAGIC - `silver_price_scd` is SCD Type 2 with exactly one current version (`is_current`) per `product_id`, and carries `price_sk`, `effective_from` and `effective_to` conformed from the raw `silver_price_versions`.
# MAGIC - `PRD-007` has exactly two price versions in `silver_price_scd` by batch three, and its current version is the Ares incident mispricing `3435.30`.
# MAGIC - `silver_customer_scd` is SCD Type 2 with exactly one open version (`__END_AT IS NULL`) per `customer_id`.
# MAGIC - `CUST-0033` has exactly two tier versions in `silver_customer_scd`, and its current tier is `TRADE`.
# MAGIC
# MAGIC **References.** You may find these Databricks docs helpful:
# MAGIC - [The AUTO CDC APIs: Simplify change data capture with pipelines](https://docs.databricks.com/aws/en/ldp/cdc)
# MAGIC - [create_auto_cdc_flow](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes)
# MAGIC - [create_auto_cdc_from_snapshot_flow](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes-from-snapshot)
# MAGIC - [Streaming tables](https://docs.databricks.com/aws/en/dlt/streaming-tables)

# COMMAND ----------

# Your turn. Write the scd declarations in 4_7_pipeline_source/4_7_scd, then run the check cell.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Reduce the change feeds with AUTO CDC and SCD Type 2 (solution)
# MAGIC <details>
# MAGIC <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; overflow:auto;"></pre>
# MAGIC
# MAGIC <script>
# MAGIC var codeB64 = "IyBzaWx2ZXJfb3JkZXJzOiB0aGUgb3JkZXIgY2hhbmdlIGZlZWQgcmVkdWNlZCB0byBjdXJyZW50IHN0YXRlLiBBVVRPIENEQyByZXBsYWNlcyB0aGUgaGFuZCByb2xsZWQgZm9yZWFjaEJhdGNoCiMgTUVSR0UgZnJvbSBMYWIgMy44OiBvbmUgZGlyZWN0aXZlIGFwcGxpZXMgdGhlIGxhdGVzdCBjaGFuZ2UgcGVyIG9yZGVyX2lkIChvcmRlcmVkIGJ5IGNoYW5nZV9zZXEsIHNpbmNlCiMgY2hhbmdlX3RzIGNhbiBhcnJpdmUgb3V0IG9mIG9yZGVyKSwgdHVybnMgYSBERUxFVEUgaW50byBhIHNvZnQgZGVsZXRlLCBhbmQgZHJvcHMgdGhlIENEQyBvcCBjb2x1bW4uIFRoZSBkZWZhdWx0CiMgaXMgU0NEIHR5cGUgMSAoY3VycmVudCBzdGF0ZSBvbmx5KSwgd2hpY2ggaXMgd2hhdCB3ZSB3YW50IGhlcmUuCkBkcC50ZW1wb3JhcnlfdmlldygpCmRlZiBvcmRlcnNfY2RjX2NsZWFuKCk6CiAgICByZXR1cm4gKAogICAgICAgIHNwYXJrLnJlYWRTdHJlYW0udGFibGUoZiJ7YnJvbnplfS5icm9uemVfb3JkZXJzIikKICAgICAgICAuc2VsZWN0KAogICAgICAgICAgICBjb2woIm9yZGVyX2lkIikuY2FzdCgic3RyaW5nIikuYWxpYXMoIm9yZGVyX2lkIiksCiAgICAgICAgICAgIGNvbCgiY3VzdG9tZXJfaWQiKS5jYXN0KCJzdHJpbmciKS5hbGlhcygiY3VzdG9tZXJfaWQiKSwKICAgICAgICAgICAgY29sKCJ3YXJlaG91c2VfaWQiKS5jYXN0KCJzdHJpbmciKS5hbGlhcygid2FyZWhvdXNlX2lkIiksCiAgICAgICAgICAgIGNvbCgiY2hhbm5lbCIpLmNhc3QoInN0cmluZyIpLmFsaWFzKCJjaGFubmVsIiksCiAgICAgICAgICAgIGNvbCgib3JkZXJfdHMiKS5jYXN0KCJ0aW1lc3RhbXAiKS5hbGlhcygib3JkZXJfdHMiKSwKICAgICAgICAgICAgY29sKCJzdGF0dXMiKS5jYXN0KCJzdHJpbmciKS5hbGlhcygic3RhdHVzIiksCiAgICAgICAgICAgIGNvbCgib3AiKS5jYXN0KCJzdHJpbmciKS5hbGlhcygib3AiKSwKICAgICAgICAgICAgY29sKCJjaGFuZ2Vfc2VxIikuY2FzdCgibG9uZyIpLmFsaWFzKCJjaGFuZ2Vfc2VxIiksCiAgICAgICAgICAgIGNvbCgiY2hhbmdlX3RzIikuY2FzdCgidGltZXN0YW1wIikuYWxpYXMoImNoYW5nZV90cyIpLAogICAgICAgICkKICAgICkKCgpkcC5jcmVhdGVfc3RyZWFtaW5nX3RhYmxlKG5hbWU9ZiJ7c2lsdmVyfS5zaWx2ZXJfb3JkZXJzIiwKICAgICAgICAgICAgICAgICAgICAgICAgICBjb21tZW50PSJPcmRlciBjdXJyZW50IHN0YXRlLCByZWR1Y2VkIGZyb20gdGhlIENEQyBmZWVkIGJ5IEFVVE8gQ0RDIikKZHAuY3JlYXRlX2F1dG9fY2RjX2Zsb3coCiAgICB0YXJnZXQ9ZiJ7c2lsdmVyfS5zaWx2ZXJfb3JkZXJzIiwKICAgIHNvdXJjZT0ib3JkZXJzX2NkY19jbGVhbiIsCiAgICBrZXlzPVsib3JkZXJfaWQiXSwKICAgIHNlcXVlbmNlX2J5PWNvbCgiY2hhbmdlX3NlcSIpLCAgICAgICAgICAjIGhpZ2hlc3QgY2hhbmdlX3NlcSBwZXIgb3JkZXIgaXMgdGhlIGN1cnJlbnQgc3RhdGUKICAgIGFwcGx5X2FzX2RlbGV0ZXM9Y29sKCJvcCIpID09ICJERUxFVEUiLCAgIyBhIERFTEVURSBpcyBhIGNhbmNlbGxlZCBhbmQgcHVyZ2VkIG9yZGVyCiAgICBleGNlcHRfY29sdW1uX2xpc3Q9WyJvcCJdLCAgICAgICAgICAgICAgICMgdGhlIENEQyBvcCBmbGFnIGRvZXMgbm90IGJlbG9uZyBpbiBTaWx2ZXIKKQoKCiMgc2lsdmVyX3ByaWNlX3NjZDogdGhlIHByaWNlIGFuZCBjb3N0IGhpc3RvcnkgYXMgU0NEIFR5cGUgMiwgY29uZm9ybWVkIHRvIHRoZSBjb250cmFjdCBjb2x1bW5zLiBUaGUgcHJpY2UgZmVlZAojIGNhcnJpZXMgZXhwbGljaXQgdmVyc2lvbiBkYXRlcywgc28gQVVUTyBDREMgc2VxdWVuY2VzIGJ5IGVmZmVjdGl2ZV9mcm9tIGFuZCBzdG9yZXMgdHlwZSAyIGludG8KIyBzaWx2ZXJfcHJpY2VfdmVyc2lvbnM6IG9uZSBkaXJlY3RpdmUgcmVwbGFjZXMgdGhlIHdob2xlIHdpbmRvdyBwbHVzIE1FUkdFIGZyb20gTGFiIDMuOS4gVGhlIGVuZ2luZSBhZGRzCiMgX19TVEFSVF9BVCBhbmQgX19FTkRfQVQgKHNhbWUgdHlwZSBhcyBlZmZlY3RpdmVfZnJvbSk7IHRoZSBjdXJyZW50IHZlcnNpb24gaGFzIGEgbnVsbCBfX0VORF9BVC4gQSBtYXRlcmlhbGl6ZWQKIyB2aWV3IHRoZW4gbWFwcyB0aG9zZSBlbmdpbmUgY29sdW1ucyB0byB0aGUgY29udHJhY3QgbmFtZXMgKGVmZmVjdGl2ZV9mcm9tL2VmZmVjdGl2ZV90bywgaXNfY3VycmVudCkgYW5kIGJ1aWxkcwojIHRoZSBwcmljZV9zayBzdXJyb2dhdGUga2V5LCBzbyB0aGUgR29sZCBmYWN0cyBwcmljZSBwb2ludCBpbiB0aW1lIHN0cmFpZ2h0IG9mZiBTaWx2ZXIgd2l0aCBubyBzZXBhcmF0ZSBwcmljZQojIGRpbWVuc2lvbi4gRm9yIGV4YW1wbGUgUFJELTAwNyAodGhlIGluY2lkZW50IHBhcnQpIGxhbmRzIHR3byB2ZXJzaW9ucyBieSBiYXRjaCB0aHJlZTogNzYzMy45OSBlZmZlY3RpdmUKIyAyMjU3LTAzLTAxLCB0aGVuIHRoZSBBcmVzIG1pc3ByaWNpbmcgMzQzNS4zMCBlZmZlY3RpdmUgMjI1Ny0wMy0wMyAoYmVsb3cgaXRzIGNvc3QsIHNvIGl0IHNlbGxzIGF0IGEgbG9zcykuCkBkcC50ZW1wb3JhcnlfdmlldygpCmRlZiBwcmljZV9jZGNfY2xlYW4oKToKICAgIHJldHVybiAoCiAgICAgICAgc3BhcmsucmVhZFN0cmVhbS50YWJsZShmInticm9uemV9LmJyb256ZV9wcmljZV9saXN0IikKICAgICAgICAuc2VsZWN0KAogICAgICAgICAgICBjb2woInByb2R1Y3RfaWQiKS5jYXN0KCJzdHJpbmciKS5hbGlhcygicHJvZHVjdF9pZCIpLAogICAgICAgICAgICBjb2woInN1cHBsaWVyX2lkIikuY2FzdCgic3RyaW5nIikuYWxpYXMoInN1cHBsaWVyX2lkIiksCiAgICAgICAgICAgIGNvbCgidW5pdF9wcmljZSIpLmNhc3QoImRlY2ltYWwoMTIsMikiKS5hbGlhcygidW5pdF9wcmljZSIpLAogICAgICAgICAgICBjb2woInVuaXRfY29zdCIpLmNhc3QoImRlY2ltYWwoMTIsMikiKS5hbGlhcygidW5pdF9jb3N0IiksCiAgICAgICAgICAgIGNvbCgiY3VycmVuY3kiKS5jYXN0KCJzdHJpbmciKS5hbGlhcygiY3VycmVuY3kiKSwKICAgICAgICAgICAgY29sKCJlZmZlY3RpdmVfZnJvbSIpLmNhc3QoImRhdGUiKS5hbGlhcygiZWZmZWN0aXZlX2Zyb20iKSwKICAgICAgICApCiAgICApCgoKZHAuY3JlYXRlX3N0cmVhbWluZ190YWJsZShuYW1lPWYie3NpbHZlcn0uc2lsdmVyX3ByaWNlX3ZlcnNpb25zIiwKICAgICAgICAgICAgICAgICAgICAgICAgICBjb21tZW50PSJQcmljZSBhbmQgY29zdCB2ZXJzaW9ucywgU0NEIFR5cGUgMiBmcm9tIEFVVE8gQ0RDIChlbmdpbmUgX19TVEFSVF9BVC9fX0VORF9BVCkiKQpkcC5jcmVhdGVfYXV0b19jZGNfZmxvdygKICAgIHRhcmdldD1mIntzaWx2ZXJ9LnNpbHZlcl9wcmljZV92ZXJzaW9ucyIsCiAgICBzb3VyY2U9InByaWNlX2NkY19jbGVhbiIsCiAgICBrZXlzPVsicHJvZHVjdF9pZCJdLAogICAgc2VxdWVuY2VfYnk9Y29sKCJlZmZlY3RpdmVfZnJvbSIpLAogICAgc3RvcmVkX2FzX3NjZF90eXBlPTIsCikKCgojIHNpbHZlcl9wcmljZV9zY2Q6IGNvbmZvcm0gdGhlIEFVVE8gQ0RDIHZlcnNpb25zIHRvIHRoZSBjb250cmFjdCBjb2x1bW5zIGFuZCBidWlsZCB0aGUgcHJpY2Vfc2sgc3Vycm9nYXRlIGtleSwgc28KIyB0aGUgZmFjdHMgam9pbiBhIFNpbHZlciB0YWJsZSBjYXJyeWluZyBlZmZlY3RpdmVfZnJvbS9lZmZlY3RpdmVfdG8vaXNfY3VycmVudCwgdGhlIHNhbWUgc2hhcGUgYXMgdGhlIEpvYnMgYnVpbGQuCkBkcC5tYXRlcmlhbGl6ZWRfdmlldyhuYW1lPWYie3NpbHZlcn0uc2lsdmVyX3ByaWNlX3NjZCIsCiAgICAgICAgICAgICAgICAgICAgICBjb21tZW50PSJQcmljZSBhbmQgY29zdCBTQ0QgVHlwZSAyIGNvbmZvcm1lZCB0byB0aGUgY29udHJhY3QgY29sdW1ucyAod2l0aCBwcmljZV9zaykiKQpkZWYgc2lsdmVyX3ByaWNlX3NjZCgpOgogICAgcmV0dXJuICgKICAgICAgICBzcGFyay5yZWFkLnRhYmxlKGYie3NpbHZlcn0uc2lsdmVyX3ByaWNlX3ZlcnNpb25zIikKICAgICAgICAuc2VsZWN0KAogICAgICAgICAgICBjb25jYXRfd3MoIl8iLCBjb2woInByb2R1Y3RfaWQiKSwgZGF0ZV9mb3JtYXQoY29sKCJfX1NUQVJUX0FUIiksICJ5eXl5TU1kZCIpKS5hbGlhcygicHJpY2Vfc2siKSwKICAgICAgICAgICAgInByb2R1Y3RfaWQiLCAic3VwcGxpZXJfaWQiLCAidW5pdF9wcmljZSIsICJ1bml0X2Nvc3QiLCAiY3VycmVuY3kiLAogICAgICAgICAgICBjb2woIl9fU1RBUlRfQVQiKS5hbGlhcygiZWZmZWN0aXZlX2Zyb20iKSwKICAgICAgICAgICAgY29sKCJfX0VORF9BVCIpLmFsaWFzKCJlZmZlY3RpdmVfdG8iKSwKICAgICAgICAgICAgY29sKCJfX0VORF9BVCIpLmlzTnVsbCgpLmFsaWFzKCJpc19jdXJyZW50IiksCiAgICAgICAgKQogICAgKQoKCiMgc2lsdmVyX2N1c3RvbWVyX3NjZDogdGhlIGN1c3RvbWVyIGRpbWVuc2lvbiBhcyBTQ0QgVHlwZSAyIGZyb20gdGhlIGRhaWx5IHNuYXBzaG90cy4gVGhlIGN1c3RvbWVycyBmZWVkIGlzIGEKIyBiYXJlIGRhaWx5IGZ1bGwgc25hcHNob3Qgd2l0aCBubyB2ZXJzaW9uIGRhdGVzLCBzbyBpdCB1c2VzIHRoZSBzbmFwc2hvdCBmb3JtIG9mIEFVVE8gQ0RDOiBhIGZ1bmN0aW9uIGhhbmRzIHRoZQojIGVuZ2luZSBlYWNoIGRheSdzIHNuYXBzaG90IGluIG9yZGVyLCBhbmQgdGhlIGVuZ2luZSBvcGVucyBhIG5ldyB2ZXJzaW9uIHdoZW5ldmVyIGEgY3VzdG9tZXIncyBhdHRyaWJ1dGVzIGNoYW5nZQojIChoZXJlIGEgdGllciBwcm9tb3Rpb24pLiBUaGlzIGNvbnRyYXN0IHdpdGggdGhlIHByaWNlIGZlZWQgKGV4cGxpY2l0IHZlcnNpb24gZGF0ZXMpIGlzIHRoZSB0ZWFjaGluZyBwb2ludC4gVGhlCiMgdmVyc2lvbiBpcyB0aGUgc25hcHNob3QgZGF0ZSBhcyBhIHl5eXlNTWRkIGludGVnZXIsIHNvIF9fU1RBUlRfQVQgYW5kIF9fRU5EX0FUIHJlYWQgYXMgZGF0ZXMgZG93bnN0cmVhbS4KZGVmIG5leHRfY3VzdG9tZXJfc25hcHNob3QobGF0ZXN0X3ZlcnNpb24pOgogICAgIyBSZWFkIHRoZSBkYWlseSBzbmFwc2hvdHMgc3RyYWlnaHQgZnJvbSB0aGUgbGFuZGluZyBWb2x1bWUsIG5vdCBicm9uemVfY3VzdG9tZXJzOiBhIHBpcGVsaW5lIGRhdGFzZXQKICAgICMgY2Fubm90IGJlIHJlZmVyZW5jZWQgaW5zaWRlIHRoaXMgc25hcHNob3Qgc291cmNlIGZ1bmN0aW9uIChpdCBpcyBub3QgYSBkYXRhc2V0IHF1ZXJ5IGRlZmluaXRpb24pLgogICAgIyBUaGUgZW5naW5lIGNhbGxzIHRoaXMgb25jZSBwZXIgc25hcHNob3QgYW5kIG5lZWRzIGEgcGxhaW4gUHl0aG9uIHZlcnNpb24gYmFjaywgc28gY2hvb3NlIHRoZSBuZXh0CiAgICAjIHNuYXBzaG90IGZyb20gdGhlIGZvbGRlciBuYW1lcyBvbiB0aGUgVm9sdW1lIChjdXN0b21lcnMvc25hcHNob3RfPFlZWVktTU0tREQ+KSwgd2hpY2ggaXMgaG93IHRoZQogICAgIyBEYXRhYnJpY2tzIGV4YW1wbGVzIGRvIGl0LiBBIFNwYXJrIGFjdGlvbiBoZXJlIChjb2xsZWN0LCBmaXJzdCwgY291bnQpIHdvdWxkIHJ1biBhIGpvYiBvbiB0aGUgZHJpdmVyCiAgICAjIGV2ZXJ5IGl0ZXJhdGlvbiwgYW5kIHBpcGVsaW5lIHNvdXJjZSBpcyBjaGVja2VkIGZvciBleGFjdGx5IHRoYXQuIEEgdmVyc2lvbiBpcyB0aGUgc25hcHNob3QgZGF0ZSBhcyBhCiAgICAjIHl5eXlNTWRkIGludGVnZXIuCiAgICBzbmFwc2hvdF9kaXIgPSBmIntsYW5kaW5nfS9jdXN0b21lcnMiCiAgICBmb2xkZXJzID0ge2ludChuYW1lLnNwbGl0KCJfIiwgMSlbMV0ucmVwbGFjZSgiLSIsICIiKSk6IG5hbWUKICAgICAgICAgICAgICAgZm9yIG5hbWUgaW4gb3MubGlzdGRpcihzbmFwc2hvdF9kaXIpIGlmIG5hbWUuc3RhcnRzd2l0aCgic25hcHNob3RfIil9CiAgICBwZW5kaW5nID0gc29ydGVkKHYgZm9yIHYgaW4gZm9sZGVycyBpZiBsYXRlc3RfdmVyc2lvbiBpcyBOb25lIG9yIHYgPiBsYXRlc3RfdmVyc2lvbikKICAgIGlmIG5vdCBwZW5kaW5nOgogICAgICAgIHJldHVybiBOb25lCiAgICB2ZXJzaW9uID0gcGVuZGluZ1swXQogICAgc25hcHNob3QgPSAoCiAgICAgICAgc3BhcmsucmVhZC5wYXJxdWV0KGYie3NuYXBzaG90X2Rpcn0ve2ZvbGRlcnNbdmVyc2lvbl19IikKICAgICAgICAuc2VsZWN0KAogICAgICAgICAgICBjb2woImN1c3RvbWVyX2lkIikuY2FzdCgic3RyaW5nIikuYWxpYXMoImN1c3RvbWVyX2lkIiksCiAgICAgICAgICAgIGNvbCgiY3VzdG9tZXJfbmFtZSIpLmNhc3QoInN0cmluZyIpLmFsaWFzKCJjdXN0b21lcl9uYW1lIiksCiAgICAgICAgICAgIGNvbCgiY3VzdG9tZXJfdHlwZSIpLmNhc3QoInN0cmluZyIpLmFsaWFzKCJjdXN0b21lcl90eXBlIiksCiAgICAgICAgICAgIGNvbCgidGllciIpLmNhc3QoInN0cmluZyIpLmFsaWFzKCJ0aWVyIiksCiAgICAgICAgICAgIGNvbCgiaG9tZV93YXJlaG91c2VfaWQiKS5jYXN0KCJzdHJpbmciKS5hbGlhcygiaG9tZV93YXJlaG91c2VfaWQiKSwKICAgICAgICAgICAgY29sKCJzaWdudXBfZGF0ZSIpLmNhc3QoImRhdGUiKS5hbGlhcygic2lnbnVwX2RhdGUiKSwKICAgICAgICApCiAgICApCiAgICByZXR1cm4gKHNuYXBzaG90LCB2ZXJzaW9uKQoKCmRwLmNyZWF0ZV9zdHJlYW1pbmdfdGFibGUobmFtZT1mIntzaWx2ZXJ9LnNpbHZlcl9jdXN0b21lcl9zY2QiLAogICAgICAgICAgICAgICAgICAgICAgICAgIGNvbW1lbnQ9IkN1c3RvbWVyIGRpbWVuc2lvbiBhcyBTQ0QgVHlwZSAyICh0aWVyIGhpc3RvcnkgZnJvbSBkYWlseSBzbmFwc2hvdHMpIikKZHAuY3JlYXRlX2F1dG9fY2RjX2Zyb21fc25hcHNob3RfZmxvdygKICAgIHRhcmdldD1mIntzaWx2ZXJ9LnNpbHZlcl9jdXN0b21lcl9zY2QiLAogICAgc291cmNlPW5leHRfY3VzdG9tZXJfc25hcHNob3QsCiAgICBrZXlzPVsiY3VzdG9tZXJfaWQiXSwKICAgIHN0b3JlZF9hc19zY2RfdHlwZT0yLAop";
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
# MAGIC Confirms current-state orders with soft deletes gone, and the two SCD Type 2 invariants with the pinned PRD-007 and CUST-0033 versions.

# COMMAND ----------

print("Pipeline run:", run_pipeline())

from pyspark.sql.functions import row_number
from pyspark.sql import Window
silver = f"{catalog}.helios_silver"
bronze = f"{catalog}.helios_bronze"

orders = spark.table(f"{silver}.silver_orders")
check("silver_orders is one row per order_id", orders.count(), orders.select("order_id").distinct().count())
check_true("the CDC op flag is not in silver_orders", "op" not in orders.columns)
oid = "ORD-000001"
s_status = orders.filter(f"order_id = '{oid}'").first()["status"]
b_latest = (spark.table(f"{bronze}.bronze_orders").filter(f"order_id = '{oid}'")
            .orderBy(col("change_seq").cast("long").desc()).first())
check(f"{oid} reflects its latest CDC status", b_latest["status"], s_status)
latest_change = (spark.table(f"{bronze}.bronze_orders")
                 .withColumn("_rn", row_number().over(
                     Window.partitionBy("order_id").orderBy(col("change_seq").cast("long").desc())))
                 .filter("_rn = 1"))
deleted = latest_change.filter("op = 'DELETE'").select("order_id")
check("no soft deleted order survives in silver_orders", 0,
      orders.select("order_id").join(deleted, "order_id", "inner").count())

# Price SCD Type 2 conformed in Silver: silver_price_versions carries the engine's __START_AT/__END_AT, and the
# silver_price_scd MV maps them to the contract columns (effective_from/effective_to, is_current) with a price_sk.
price = spark.table(f"{silver}.silver_price_scd")
check_true("silver_price_scd carries the conformed contract columns",
           {"price_sk", "effective_from", "effective_to", "is_current"}.issubset(set(price.columns)))
check("exactly one current price version per product", 0,
      price.filter("is_current").groupBy("product_id").count().filter("count <> 1").count())
prd7 = price.filter("product_id = 'PRD-007'")
check("PRD-007 has two price versions by batch three", 2, prd7.count())
check("PRD-007 current price is the incident mispricing 3435.30", 3435.30,
      float(prd7.filter("is_current").first()["unit_price"]))

# Customer tier SCD Type 2.
cust = spark.table(f"{silver}.silver_customer_scd")
check("exactly one open version per customer", 0,
      cust.filter("__END_AT IS NULL").groupBy("customer_id").count().filter("count <> 1").count())
c33 = cust.filter("customer_id = 'CUST-0033'")
check("CUST-0033 has two tier versions", 2, c33.count())
check("CUST-0033 current tier is TRADE", "TRADE", c33.filter("__END_AT IS NULL").first()["tier"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## What you built
# MAGIC
# MAGIC The **scd** layer of the Helios medallion, declared in Lakeflow and built by the engine. You wrote the declarations, ran the one pipeline, and validated this layer from here.