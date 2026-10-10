# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 3.11: Orchestrate the medallion as one Job
# MAGIC
# MAGIC ### Ticket: HELIOS-311
# MAGIC
# MAGIC **Context.** The layers work, but right now they only run when you run cells by hand. Production needs one Job that runs the pipeline in the right order, in parallel where it can, retries on transient failure, runs on a schedule, and alerts someone when it breaks. You will build that Job from code with the Databricks SDK (the Jobs & Pipelines UI does the same thing, and the self checks read the Job back so either way works).
# MAGIC
# MAGIC The runnable steps are the pure pipeline task notebooks in `job_tasks/` (01_bronze, 02_silver, 03_scd, 04_gold, 05_self_check). They read from the landing Volume and assume the source has already landed. Landing the source is a separate concern; the bootstrap below does it so the Job has data to process.
# MAGIC
# MAGIC **Task.**
# MAGIC 1. Define the task graph (dependencies, retries) and create the Job.
# MAGIC 2. Add a schedule and a failure alert.
# MAGIC 3. Run it now and confirm, via the SDK, that it succeeded and Gold is correct.
# MAGIC
# MAGIC **Acceptance criteria** (the self check cells):
# MAGIC - The Job exists with five tasks wired as bronze, then silver and scd in parallel, then gold, then self_check.
# MAGIC - A paused schedule and a failure email are configured.
# MAGIC - A run finishes SUCCESS, every task finishes SUCCESS, and the Gold star schema is correct.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bootstrap
# MAGIC Lands the source so the Job has something to process, and sets up the SDK client. It resets the three medallion schemas and lands batches one to three (so the run produces a complete Gold, returns included), then defines `find_job()` and the workspace folder of the `job_tasks/` notebooks. The Job itself runs the **pure** pipeline tasks; landing the source is the simulation the bootstrap does.

# COMMAND ----------

# MAGIC %run ../00_setup/data_generator

# COMMAND ----------

# MAGIC %run ../00_setup/bootstrap_helpers

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

catalog = helios_identity()
username = spark.sql("SELECT current_user()").first()[0]
w = WorkspaceClient()

# Land the source for the Job to process (the Job tasks are pure ETL and do not land anything themselves).
for layer in ["helios_bronze", "helios_silver", "helios_gold"]:
    reset_schema(catalog, layer)
clear_landing(catalog)
ensure_landing(catalog, up_to_batch=3)

# Resolve the workspace folder of this notebook so the Job can point at the sibling job_tasks notebooks.
_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
tasks_dir = _ctx.notebookPath().get().rsplit("/", 1)[0] + "/job_tasks"
job_name = f"helios_medallion_{catalog}"


def find_job():
    """Look the Job up by name, so the self checks pass whether you built it with the SDK or in the UI."""
    return next(iter(w.jobs.list(name=job_name)), None)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Define the Job and its task graph
# MAGIC
# MAGIC Turn the five pipeline notebooks into one Job with the right shape, and capture its `job_id`. Build it either from code with the Databricks SDK or by hand in the Jobs & Pipelines UI. Both approaches are shown in the reveals below, and the self check passes whichever you choose.
# MAGIC
# MAGIC **How.**
# MAGIC - Wire the tasks so `bronze` runs first; `silver` and `scd` both depend on it (so they run in parallel); `gold` depends on both; `self_check` runs last.
# MAGIC - Give every task a couple of retries so a transient blip does not fail the whole run.
# MAGIC - Free Edition allows up to five concurrent job tasks, and this graph never runs more than two at once, so there is headroom.
# MAGIC - Make it safe to re-run by deleting any existing Job of the same name before you create it, so running the lab twice does not leave duplicates behind.
# MAGIC
# MAGIC **Example.** The five tasks wired into this shape (each points at one `job_tasks/` notebook):
# MAGIC
# MAGIC ```
# MAGIC bronze --> silver --+
# MAGIC        \-> scd ------+--> gold --> self_check
# MAGIC ```
# MAGIC
# MAGIC **Definition of done.**
# MAGIC - A Job named `helios_medallion_<catalog>` exists with five tasks: `bronze`, `silver`, `scd`, `gold`, `self_check`.
# MAGIC - `silver` and `scd` depend on `bronze`; `gold` depends on both `silver` and `scd`; `self_check` depends on `gold`.
# MAGIC - Every task has `max_retries = 2`.
# MAGIC - Re-running the cell replaces the Job rather than creating a duplicate, and you have its `job_id`.
# MAGIC
# MAGIC **References.** You may find these Databricks docs helpful:
# MAGIC - [Lakeflow Jobs](https://docs.databricks.com/aws/en/jobs/)
# MAGIC - [Configure task dependencies](https://docs.databricks.com/aws/en/jobs/run-if)
# MAGIC - [Create your first workflow with Lakeflow Jobs](https://docs.databricks.com/aws/en/jobs/jobs-quickstart)
# MAGIC - [Databricks SDK for Python](https://docs.databricks.com/aws/en/dev-tools/sdk-python)

# COMMAND ----------

# Your turn. Build the five tasks with the SDK and create the Job (capture job_id).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Task 1: Define the Job and its task graph (solution)
# MAGIC <details>
# MAGIC <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; overflow:auto;"></pre>
# MAGIC
# MAGIC <script>
# MAGIC var codeB64 = "ZGVmIG5iX3Rhc2soa2V5LCBub3RlYm9vaywgZGVwcz1Ob25lKToKICAgIHJldHVybiBqb2JzLlRhc2soCiAgICAgICAgdGFza19rZXk9a2V5LAogICAgICAgIG5vdGVib29rX3Rhc2s9am9icy5Ob3RlYm9va1Rhc2sobm90ZWJvb2tfcGF0aD1mInt0YXNrc19kaXJ9L3tub3RlYm9va30iLCBzb3VyY2U9am9icy5Tb3VyY2UuV09SS1NQQUNFKSwKICAgICAgICBkZXBlbmRzX29uPVtqb2JzLlRhc2tEZXBlbmRlbmN5KHRhc2tfa2V5PWQpIGZvciBkIGluIChkZXBzIG9yIFtdKV0sCiAgICAgICAgbWF4X3JldHJpZXM9MiwgbWluX3JldHJ5X2ludGVydmFsX21pbGxpcz0zMF8wMDAsCiAgICApCgoKdGFza3MgPSBbbmJfdGFzaygiYnJvbnplIiwgIjAxX2Jyb256ZSIpLAogICAgICAgICBuYl90YXNrKCJzaWx2ZXIiLCAiMDJfc2lsdmVyIiwgWyJicm9uemUiXSksCiAgICAgICAgIG5iX3Rhc2soInNjZCIsICIwM19zY2QiLCBbImJyb256ZSJdKSwKICAgICAgICAgbmJfdGFzaygiZ29sZCIsICIwNF9nb2xkIiwgWyJzaWx2ZXIiLCAic2NkIl0pLAogICAgICAgICBuYl90YXNrKCJzZWxmX2NoZWNrIiwgIjA1X3NlbGZfY2hlY2siLCBbImdvbGQiXSldCgpmb3IgZXhpc3RpbmcgaW4gdy5qb2JzLmxpc3QobmFtZT1qb2JfbmFtZSk6ICAgICAjIHJlcGxhY2UgYW55IGV4aXN0aW5nIEpvYiBvZiB0aGlzIG5hbWUKICAgIHcuam9icy5kZWxldGUoam9iX2lkPWV4aXN0aW5nLmpvYl9pZCkKCmpvYl9pZCA9IHcuam9icy5jcmVhdGUobmFtZT1qb2JfbmFtZSwgdGFza3M9dGFza3MsIG1heF9jb25jdXJyZW50X3J1bnM9MSkuam9iX2lkCnByaW50KGYiQ3JlYXRlZCBKb2Ige2pvYl9pZH06IHtqb2JfbmFtZX0iKQ==";
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

# MAGIC %md-sandbox
# MAGIC ##### Task 1: Define the Job and its task graph (Jobs & Pipelines UI) (solution)
# MAGIC <details>
# MAGIC <summary>EXPAND FOR SOLUTION STEPS (Jobs & Pipelines UI)</summary>
# MAGIC
# MAGIC You do not have to use the SDK; the same Job can be built by hand in the Jobs & Pipelines UI.
# MAGIC
# MAGIC 1. In the left sidebar open Jobs & Pipelines, then Create Job.
# MAGIC 2. Name it helios_medallion_(your catalog).
# MAGIC 3. Add a task bronze, type Notebook, pointing at job_tasks/01_bronze. Set Retries to 2.
# MAGIC 4. Add silver (job_tasks/02_silver) and scd (job_tasks/03_scd), each Depends on bronze.
# MAGIC 5. Add gold (job_tasks/04_gold), Depends on silver and scd.
# MAGIC 6. Add self_check (job_tasks/05_self_check), Depends on gold.
# MAGIC 7. Save. The graph shows bronze fanning out to silver and scd, then converging on gold, then self_check.
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Self check: Task 1

# COMMAND ----------

job = find_job()
check_true("the medallion Job exists", job is not None)
info = w.jobs.get(job_id=job.job_id)
check("the Job has five tasks", 5, len(info.settings.tasks))
deps = {t.task_key: sorted(d.task_key for d in (t.depends_on or [])) for t in info.settings.tasks}
check("silver depends on bronze", ["bronze"], deps.get("silver"))
check("scd depends on bronze", ["bronze"], deps.get("scd"))
check("gold depends on silver and scd", ["scd", "silver"], deps.get("gold"))
check("self_check depends on gold", ["gold"], deps.get("self_check"))
check_true("every task has retries", all((t.max_retries or 0) >= 2 for t in info.settings.tasks))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Add a schedule and a failure alert
# MAGIC
# MAGIC Give the Job a daily schedule (paused, so this lab triggers runs on demand) and an email alert on failure, so a real pipeline runs itself and tells someone when it breaks.
# MAGIC
# MAGIC **How.**
# MAGIC - Update the Job to add a cron schedule, daily, and leave it `PAUSED` (you will trigger runs by hand in this lab; in production you would unpause it).
# MAGIC - Add an email notification on failure, sent to your own login.
# MAGIC - Leave the five tasks you already defined in place.
# MAGIC
# MAGIC **Definition of done.**
# MAGIC - The Job has a daily cron schedule with `pause_status = PAUSED`.
# MAGIC - A failure email notification is set to your user.
# MAGIC - The five tasks defined in Task 1 are still in place.
# MAGIC
# MAGIC **References.** You may find these Databricks docs helpful:
# MAGIC - [Automate jobs with schedules and triggers](https://docs.databricks.com/aws/en/jobs/triggers)
# MAGIC - [Add notifications on a job](https://docs.databricks.com/aws/en/jobs/notifications)
# MAGIC - [Lakeflow Jobs](https://docs.databricks.com/aws/en/jobs/)
# MAGIC - [Databricks SDK for Python](https://docs.databricks.com/aws/en/dev-tools/sdk-python)

# COMMAND ----------

# Your turn. Add a paused daily schedule and a failure email to the Job.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Task 2: Add a schedule and a failure alert (solution)
# MAGIC <details>
# MAGIC <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; overflow:auto;"></pre>
# MAGIC
# MAGIC <script>
# MAGIC var codeB64 = "am9iID0gZmluZF9qb2IoKQppbmZvID0gdy5qb2JzLmdldChqb2JfaWQ9am9iLmpvYl9pZCkKdy5qb2JzLnVwZGF0ZSgKICAgIGpvYl9pZD1qb2Iuam9iX2lkLAogICAgbmV3X3NldHRpbmdzPWpvYnMuSm9iU2V0dGluZ3MoCiAgICAgICAgdGFza3M9aW5mby5zZXR0aW5ncy50YXNrcywgICAgICAgICAgICAjIGtlZXAgdGhlIHRhc2sgZ3JhcGggZnJvbSBUYXNrIDEKICAgICAgICBzY2hlZHVsZT1qb2JzLkNyb25TY2hlZHVsZSgKICAgICAgICAgICAgcXVhcnR6X2Nyb25fZXhwcmVzc2lvbj0iMCAwIDYgKiAqID8iLCAgICAgIyAwNjowMCBkYWlseQogICAgICAgICAgICB0aW1lem9uZV9pZD0iVVRDIiwKICAgICAgICAgICAgcGF1c2Vfc3RhdHVzPWpvYnMuUGF1c2VTdGF0dXMuUEFVU0VELCAgICAgIyBwYXVzZWQ6IHRoaXMgbGFiIHRyaWdnZXJzIHJ1bnMgb24gZGVtYW5kCiAgICAgICAgKSwKICAgICAgICBlbWFpbF9ub3RpZmljYXRpb25zPWpvYnMuSm9iRW1haWxOb3RpZmljYXRpb25zKG9uX2ZhaWx1cmU9W3VzZXJuYW1lXSksCiAgICApLAopCnByaW50KCJTY2hlZHVsZSBhbmQgZmFpbHVyZSBhbGVydCBhZGRlZCIp";
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

# MAGIC %md-sandbox
# MAGIC ##### Task 2: Add a schedule and a failure alert (Jobs & Pipelines UI) (solution)
# MAGIC <details>
# MAGIC <summary>EXPAND FOR SOLUTION STEPS (Jobs & Pipelines UI)</summary>
# MAGIC
# MAGIC You do not have to use the SDK; the same schedule and failure alert can be added by hand in the Jobs & Pipelines UI.
# MAGIC
# MAGIC 1. In the left sidebar open Jobs & Pipelines and open the helios_medallion_(your catalog) Job from Task 1.
# MAGIC 2. In the right sidebar, under Schedules & Triggers, choose Add trigger, then set Trigger type to Scheduled.
# MAGIC 3. Set it to run daily at 06:00 with timezone UTC, then save the trigger.
# MAGIC 4. Set the trigger to Paused, so this lab triggers runs on demand rather than on the clock.
# MAGIC 5. In the right sidebar, under Notifications, choose Add, enter your own login as the recipient, and tick the failure event so an email is sent when a run fails.
# MAGIC 6. Leave the five tasks from Task 1 in place. The Job now carries a paused daily schedule and a failure alert.
# MAGIC </details>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Self check: Task 2

# COMMAND ----------

info = w.jobs.get(job_id=find_job().job_id)
check_true("a schedule is configured", info.settings.schedule is not None)
check("the schedule is paused", jobs.PauseStatus.PAUSED, info.settings.schedule.pause_status)
check_true("a failure email is configured",
           bool(info.settings.email_notifications and info.settings.email_notifications.on_failure))
check("the five tasks are still in place", 5, len(info.settings.tasks))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Run the Job and verify it succeeded
# MAGIC
# MAGIC Run the Job, wait for it to finish, then confirm from the outside that it actually worked, the run and every task succeeded, and the Job really produced Gold.
# MAGIC
# MAGIC **How.**
# MAGIC - Trigger a run and wait for it to finish.
# MAGIC - Then verify rather than trust that it returned: check the run and every task came back successful, and that the Job really produced Gold (the facts at the expected grain and the dimensions present).
# MAGIC - Checking a run from the outside like this is how you would verify a scheduled pipeline in production.
# MAGIC
# MAGIC **Example.** A successful run reports `SUCCESS` overall and for every task: `bronze`, `silver`, `scd`, `gold`, `self_check`.
# MAGIC
# MAGIC **Definition of done.**
# MAGIC - A run is triggered and reaches a terminal state of `SUCCESS`.
# MAGIC - Every task in the run finished `SUCCESS`.
# MAGIC - The Job produced Gold: `fact_order_lines` is one row per line and matches `silver_order_lines`, and the dimensions exist.
# MAGIC
# MAGIC **References.** You may find these Databricks docs helpful:
# MAGIC - [Lakeflow Jobs](https://docs.databricks.com/aws/en/jobs/)
# MAGIC - [Monitoring and observability for Lakeflow Jobs](https://docs.databricks.com/aws/en/jobs/monitor)
# MAGIC - [Databricks SDK for Python](https://docs.databricks.com/aws/en/dev-tools/sdk-python)

# COMMAND ----------

# Your turn. Run the Job, wait for it, and confirm it succeeded.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Task 3: Run the Job and verify it succeeded (solution)
# MAGIC <details>
# MAGIC <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; overflow:auto;"></pre>
# MAGIC
# MAGIC <script>
# MAGIC var codeB64 = "cnVuID0gdy5qb2JzLnJ1bl9ub3coam9iX2lkPWZpbmRfam9iKCkuam9iX2lkKS5yZXN1bHQoKSAgICAjIHRyaWdnZXIgYW5kIHdhaXQKcHJpbnQoZiJSdW4ge3J1bi5ydW5faWR9IGZpbmlzaGVkIHdpdGgge3J1bi5zdGF0ZS5yZXN1bHRfc3RhdGV9IikKZm9yIHQgaW4gcnVuLnRhc2tzOgogICAgcHJpbnQoZiIgIHt0LnRhc2tfa2V5OjwxMn0ge3Quc3RhdGUucmVzdWx0X3N0YXRlfSIp";
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
# MAGIC ### Self check: Task 3

# COMMAND ----------

# Read the latest run back through the SDK, so this validates the run whether you triggered it with run_now
# in the reveal or clicked Run now in the UI.
job = find_job()
latest = next(iter(w.jobs.list_runs(job_id=job.job_id, limit=1)), None)
check_true("the Job has at least one run", latest is not None)
final = w.jobs.get_run(latest.run_id)
check_true("the latest run finished SUCCESS", final.state.result_state == jobs.RunResultState.SUCCESS)
check_true("every task finished SUCCESS",
           all(t.state.result_state == jobs.RunResultState.SUCCESS for t in final.tasks))

# Data checks on what the Job produced, invariants, not a fixed row count.
gold = f"{catalog}.helios_gold"
silver = f"{catalog}.helios_silver"
fact = spark.table(f"{gold}.fact_order_lines")
check("the Job produced the fact at one row per line", fact.count(), fact.select("order_line_id").distinct().count())
check("the fact matches the clean line count", spark.table(f"{silver}.silver_order_lines").count(), fact.count())
check_true("all Gold dimensions exist",
           all(spark.catalog.tableExists(f"{gold}.{t}")
               for t in ["dim_product", "dim_customer", "dim_supplier", "dim_warehouse", "dim_category",
                         "dim_date"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What you built
# MAGIC
# MAGIC One Job that runs the whole medallion in the right order, `bronze` then `silver` and `scd` in parallel, then `gold`, then the `self_check` gate, with retries, a paused daily schedule, and a failure alert. You created it from code with the Databricks SDK and verified the run from the outside. The same Job can be built in the Jobs & Pipelines UI, and the self checks read it back either way.