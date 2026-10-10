# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 4.11: Triage and fix the incident
# MAGIC
# MAGIC ### Ticket: HELIOS-411 (PagerDuty)
# MAGIC
# MAGIC **Context.** You are on call. The Helios medallion pipeline, which ran clean for days, failed its scheduled run overnight and Gold did not refresh. You inherit the pipeline exactly as it runs in production. Deploy it, watch it fail, find out why from the pipeline event log, and put it right.
# MAGIC
# MAGIC **How this lab is shaped.** The inherited pipeline source lives in the `4_11_pipeline_source` folder, four notebooks one per layer (the same shape as Lab 4.5): `4_11_bronze`, `4_11_silver`, `4_11_scd`, `4_11_gold`. They are complete and you do not write them. A teammate's recent change is in there, and your fix is a one line edit to the source, but first you have to find it. You drive everything from here.
# MAGIC
# MAGIC **The task.** Find what failed in the event log, fix it, and re run the pipeline clean.
# MAGIC
# MAGIC **Acceptance criteria** (the self check): the pipeline run completes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bootstrap
# MAGIC Restores the environment and lands all three batches, the clean days and the Ares incident day. Resets the layers the pipeline owns, sets up the SDK client, resolves the four inherited source notebooks, defines `run_pipeline()`, and names the one pipeline. Gold goes to `helios_gold`, the one canonical Gold schema (the same target the Section 3 build writes). If you edited any of the `4_11_pipeline_source` notebooks on a previous pass, restore them to their inherited state before re running, so the incident is present. Run this top to bottom, then deploy the pipeline below.

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

# Reset the layers the pipeline owns and land all three batches; the Ares incident is in batch three.
for layer in ["helios_bronze", "helios_silver"]:
    reset_schema(catalog, layer)
spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.helios_gold CASCADE")
spark.sql(f"CREATE SCHEMA {catalog}.helios_gold")
clear_landing(catalog)
ensure_landing(catalog, up_to_batch=3)

# Resolve the four inherited source notebooks next to this lab, and the libraries that point the pipeline at them.
_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
_dir = _ctx.notebookPath().get().rsplit("/", 1)[0]
src_dir = f"{_dir}/4_11_pipeline_source"
source_notebooks = ["4_11_bronze", "4_11_silver", "4_11_scd", "4_11_gold"]
libraries = [pipelines.PipelineLibrary(notebook=pipelines.NotebookLibrary(path=f"{src_dir}/{nb}"))
             for nb in source_notebooks]
pipeline_name = f"helios_lsdp_{catalog}"


def find_pipeline():
    matches = w.pipelines.list_pipelines(filter=f"name LIKE '{pipeline_name}'")
    return next((p for p in matches if p.name == pipeline_name), None)


def run_pipeline():
    """Trigger an update on the one pipeline and wait for a terminal state. Returns the state."""
    pid = find_pipeline().pipeline_id
    update_id = w.pipelines.start_update(pipeline_id=pid).update_id
    terminal = {"COMPLETED", "FAILED", "CANCELED"}
    while True:
        update = w.pipelines.get_update(pipeline_id=pid, update_id=update_id).update
        state = update.state.value if update.state else "PENDING"
        if state in terminal:
            return state
        time.sleep(15)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy the inherited pipeline
# MAGIC
# MAGIC Stand up the pipeline exactly as you inherited it and trigger a run. You have not changed anything, the bug is already in the source, so the run fails: the same failure that paged you. Run the cell below, confirm it is red, then go and find the cause.

# COMMAND ----------

# Deploy the inherited pipeline exactly as it runs in production (one active pipeline per type on Free Edition,
# so replace any existing one), then trigger a run. You are not changing anything, you are running what you
# inherited, so the run fails: that is the incident you were paged about.
existing = find_pipeline()
if existing:
    w.pipelines.delete(pipeline_id=existing.pipeline_id)
created = w.pipelines.create(
    name=pipeline_name, serverless=True, continuous=False,
    catalog=catalog, schema="helios_bronze",
    configuration={"helios.catalog": catalog},
    libraries=libraries,
)
print(f"Deployed pipeline {created.pipeline_id}: {pipeline_name}")
state = run_pipeline()
print(f"Run state: {state}")
print("The pipeline is FAILED. Find the cause and fix it in the task below.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Find the failure, fix it, and re run the pipeline clean
# MAGIC
# MAGIC The pipeline is red and Gold did not refresh, and you did not write any of it. Every pipeline writes an event log, and it is the first place an on call engineer looks: it records each run, each dataset the run touched, and the error that stopped it. Find the failure there, change the one line that caused it, and get a clean run.
# MAGIC
# MAGIC **How.**
# MAGIC - Read this pipeline's event log (via the UI or the `event_log()` table valued function, passing the pipeline id (`find_pipeline().pipeline_id` gives it to you)).
# MAGIC - Find the issue it reports: the dataset that failed, and what broke it.
# MAGIC - Correct the code in `4_11_pipeline_source` and save that notebook.
# MAGIC - Re run the pipeline by running the self check below.
# MAGIC
# MAGIC **Definition of done.**
# MAGIC - The pipeline run completes.
# MAGIC
# MAGIC **References.** You may find these Databricks docs helpful:
# MAGIC - [Pipeline event log](https://docs.databricks.com/aws/en/ldp/monitor-event-logs)
# MAGIC - [Manage data quality with pipeline expectations](https://docs.databricks.com/aws/en/dlt/expectations)
# MAGIC - [Run a pipeline update](https://docs.databricks.com/aws/en/ldp/updates)

# COMMAND ----------

# Your turn. Read this pipeline's event log to find what failed the run, then correct the code in
# 4_11_pipeline_source and run the self check below.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Task 1: Find the failure, fix it, and re run the pipeline clean (solution)
# MAGIC <details>
# MAGIC <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; overflow:auto;"></pre>
# MAGIC
# MAGIC <script>
# MAGIC var codeB64 = "IyBUaGUgZXZlbnQgbG9nIHBvaW50cyBhdCB0aGUgR29sZCB0YWJsZSBmYWN0X29yZGVyX2xpbmVzLCBmYWlsaW5nIG9uIHRoaXMgZXhwZWN0YXRpb24gaW4KIyA0XzExX3BpcGVsaW5lX3NvdXJjZS80XzExX2dvbGQ6CiMKIyAgICAgQGRwLmV4cGVjdF9vcl9mYWlsKCJub25fbmVnYXRpdmVfbWFyZ2luIiwgIm1hcmdpbiA+PSAwIikKIwojIFRoZSByb3dzIGl0IHJlamVjdGVkIGFyZSBQUkQtMDA3IGZyb20gc3VwcGxpZXIgU1VQLTExLCBwcmljZWQgYmVsb3cgY29zdCBvbiAyMjU3LTAzLTAzOiBvbmUgcHJvZHVjdCwgb25lCiMgc3VwcGxpZXIsIG9uZSBkYXksIHNvIHRoaXMgaXMgdGhlIEFyZXMgcHJpY2luZyBpbmNpZGVudCwgYSByZWFsIGJ1c2luZXNzIGV2ZW50IHJhdGhlciB0aGFuIGNvcnJ1cHQgZGF0YS4KIyBUaGUgZGF0YSBpcyByaWdodCBhbmQgdGhlIHJ1bGUgaXMgdG9vIHN0cmljdCwgYW5kIHRoZSBpbmNpZGVudCBoYXMgdG8gcmVhY2ggR29sZCBmb3IgU2VjdGlvbnMgNSBhbmQgNiB0bwojIGludmVzdGlnYXRlLCBzbyBzb2Z0ZW4gdGhlIGdhdGUgdG8gYSB3YXJuaW5nLCB3aGljaCBrZWVwcyB0aGUgcm93cyBhbmQgcmVjb3JkcyB0aGUgY291bnQ6CiMKIyAgICAgQGRwLmV4cGVjdCgibm9uX25lZ2F0aXZlX21hcmdpbiIsICJtYXJnaW4gPj0gMCIpCiMKIyBTYXZlIDRfMTFfZ29sZCwgdGhlbiBydW4gdGhlIHNlbGYgY2hlY2sgYmVsb3cgdG8gcmUgcnVuIHRoZSBwaXBlbGluZS4=";
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
# MAGIC ### Self check
# MAGIC Re runs the pipeline with your fix in place and confirms it completes.

# COMMAND ----------

# This re runs the pipeline, so it takes a couple of minutes. It passes once your fix is saved.
check("the pipeline run completed after the fix", "COMPLETED", run_pipeline())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recovering safely
# MAGIC
# MAGIC Two operational controls are worth knowing while you are still in the middle of an incident. Development mode keeps the pipeline's compute warm between runs, so each retry starts fast, while production mode tears the compute down after every run and is what a scheduled pipeline uses. And when a change is confined to one table you can refresh just that table instead of everything, though a change to a stateful streaming query's logic (its watermark threshold, or its dedup or aggregation keys) needs a full refresh, because the existing checkpoint state no longer matches the new logic. That is why the medallion keeps Bronze raw and append only: Silver and Gold can always be rebuilt from it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What you built
# MAGIC
# MAGIC A clean incident response on a declarative pipeline. You inherited a failing pipeline you did not write, read its event log to find the dataset that failed and the rule that stopped the run, and traced the cause to a single below cost price on one product, one supplier and one day, which reads as a real business event rather than corrupt data. You chose the right severity, warn rather than fail or drop, changed the one decorator a teammate got wrong, and re ran the pipeline clean with the Ares rows landing in Gold where the analysts can still see them.