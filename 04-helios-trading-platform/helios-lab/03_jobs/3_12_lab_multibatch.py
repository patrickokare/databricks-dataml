# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Lab 3.12: Run the pipeline across five batches
# MAGIC
# MAGIC ### Ticket: HELIOS-312
# MAGIC
# MAGIC **Context.** You have built the medallion and orchestrated it as one Job. The last thing a real team does before trusting a pipeline is run it the way production will: the source keeps dropping new data, and the same Job runs again and again, staying correct as the tables grow. This lab does exactly that across five batches of the simulator.
# MAGIC
# MAGIC **There is nothing to write here.** Run each cell in order and read what comes back. After every batch you will see the medallion reconcile against the source with all checks passing, and a short summary of the numbers, and by the end you will see a real incident surface: a pricing error at Ares Depot on the batch three day, the thing the Section 5 dashboards and the Section 6 app are built to investigate.
# MAGIC
# MAGIC **What you should see.**
# MAGIC - Each of the five Job runs finishes SUCCESS.
# MAGIC - After every batch the checks pass: Silver and the facts match the clean landed source, the order line fact stays one row per line, every line is priced, and the price history keeps one current row per product.
# MAGIC - A clear spike in the incident product at Ares on the batch three day, sold below cost (negative margin).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: recreate the Job at its required state
# MAGIC
# MAGIC Load the generator and helpers, derive your catalog, and recreate the medallion Job from Lab 3.11 at its full required state (the five-task graph with retries, a paused daily schedule, and a failure alert), replacing any existing Job of the same name. This is setup, so it is given to you; just run it.

# COMMAND ----------

# MAGIC %run ../00_setup/data_generator

# COMMAND ----------

# MAGIC %run ../00_setup/bootstrap_helpers

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
from pyspark.sql.functions import col, row_number
from pyspark.sql import Window

catalog = helios_identity()
username = spark.sql("SELECT current_user()").first()[0]
w = WorkspaceClient()

_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
tasks_dir = _ctx.notebookPath().get().rsplit("/", 1)[0] + "/job_tasks"
job_name = f"helios_medallion_{catalog}"


def nb_task(key, notebook, deps=None):
    return jobs.Task(
        task_key=key,
        notebook_task=jobs.NotebookTask(notebook_path=f"{tasks_dir}/{notebook}", source=jobs.Source.WORKSPACE),
        depends_on=[jobs.TaskDependency(task_key=d) for d in (deps or [])],
        max_retries=2, min_retry_interval_millis=30_000,
    )


tasks = [nb_task("bronze", "01_bronze"),
         nb_task("silver", "02_silver", ["bronze"]),
         nb_task("scd", "03_scd", ["bronze"]),
         nb_task("gold", "04_gold", ["silver", "scd"]),
         nb_task("self_check", "05_self_check", ["gold"])]

for existing in w.jobs.list(name=job_name):     # replace any existing Job of this name
    w.jobs.delete(job_id=existing.job_id)

job_id = w.jobs.create(
    name=job_name, tasks=tasks, max_concurrent_runs=1,
    schedule=jobs.CronSchedule(quartz_cron_expression="0 0 6 * * ?", timezone_id="UTC",
                               pause_status=jobs.PauseStatus.PAUSED),
    email_notifications=jobs.JobEmailNotifications(on_failure=[username]),
).job_id
print(f"Job {job_id} created at required state: {job_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean slate: start from nothing
# MAGIC
# MAGIC This is a full end-to-end run, so start from an empty slate: clear the landing Volume and reset the three medallion schemas. The streaming checkpoints live inside `helios_silver`, so resetting it makes the first batch ingest cleanly and every later batch build incrementally on top.

# COMMAND ----------

clear_landing(catalog)
for layer in ["helios_bronze", "helios_silver", "helios_gold"]:
    reset_schema(catalog, layer)
print("Landing cleared and medallion schemas reset. Starting from an empty slate.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## How each batch is checked
# MAGIC
# MAGIC After every batch you reconcile the medallion against everything that has landed so far. The check below reads the landed `order_lines` files, dedupes them on `order_line_id` and drops the contract-breakers (non positive quantity, missing product) to get the clean source count, then confirms Silver and the fact both match it, the fact did not fan out, every line is priced, and the price history keeps one current row per product. It is derived from the source rather than hardcoded, so it holds however many batches have run. You do not write it; it is here so you can see how the run is verified.

# COMMAND ----------

def check_medallion(catalog, b):
    """Reconcile the medallion against the landed source after batch b, and assert the invariants."""
    silver = f"{catalog}.helios_silver"
    gold   = f"{catalog}.helios_gold"
    root   = f"/Volumes/{catalog}/helios_raw/helios_landing"

    landed = (
        spark.read.option("recursiveFileLookup", "true").json(f"{root}/order_lines")
        .select(col("order_line_id").cast("string").alias("order_line_id"),
                col("line_ts").cast("timestamp").alias("line_ts"),
                col("quantity").cast("int").alias("quantity"),
                col("product_id").cast("string").alias("product_id"))
    )
    latest = Window.partitionBy("order_line_id").orderBy(col("line_ts").desc_nulls_last())
    deduped = landed.withColumn("_rn", row_number().over(latest)).filter("_rn = 1").drop("_rn")
    expected_clean = deduped.filter("quantity IS NOT NULL AND quantity > 0 AND product_id IS NOT NULL").count()

    lines = spark.table(f"{silver}.silver_order_lines")
    fact  = spark.table(f"{gold}.fact_order_lines")
    price = spark.table(f"{silver}.silver_price_scd")

    check(f"[batch {b}] silver_order_lines matches the clean source", expected_clean, lines.count())
    check(f"[batch {b}] no duplicate order_line_id in silver", lines.count(), lines.select("order_line_id").distinct().count())
    check(f"[batch {b}] fact matches the clean line count", expected_clean, fact.count())
    check(f"[batch {b}] fact did not fan out", fact.count(), fact.select("order_line_id").distinct().count())
    check(f"[batch {b}] every line is priced", 0, fact.filter("price_sk IS NULL").count())
    check(f"[batch {b}] one current price per product", 0,
          price.filter("is_current").groupBy("product_id").count().filter("count <> 1").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run all five batches
# MAGIC
# MAGIC Now run the pipeline forward through the five batches. For each one the cell lands the batch (the source dropping new data), triggers the Job and waits for it, confirms the run succeeded, reconciles the medallion, and prints a short revenue and margin summary so you can watch the numbers grow. This runs the real Job five times, so it takes a few minutes. Run it and watch each batch go green.

# COMMAND ----------

gold = f"{catalog}.helios_gold"

# A plain reporting query (a display, not pipeline logic): revenue and margin by date.
summary = f"""
    SELECT d.date,
           ROUND(SUM(f.gross_amount), 0) AS revenue,
           ROUND(SUM(f.margin), 0)       AS margin
    FROM {gold}.fact_order_lines f
    JOIN {gold}.dim_date d ON f.date_key = d.date_key
    GROUP BY d.date
    ORDER BY d.date
"""

for b in range(1, 6):
    print(f"\n===== batch {b} =====")
    generate_batch(catalog, batch=b)                  # the source drops batch b
    run = w.jobs.run_now(job_id=job_id).result()      # run the orchestrated Job, wait for it
    check_true(f"[batch {b}] the Job run finished SUCCESS",
               run.state.result_state == jobs.RunResultState.SUCCESS)
    check_medallion(catalog, b)                        # reconcile the medallion to the source
    display(spark.sql(summary))                        # high level numbers

# COMMAND ----------

# MAGIC %md
# MAGIC ## The incident in the numbers
# MAGIC
# MAGIC The medallion reconciled after every batch. Now look at the story the numbers tell. The summary below is the incident product (`PRD-007`, a propulsion part from supplier `SUP-11`) at Ares Depot (`DEP-03`) by date. On the batch three day a pricing error drops its price below cost, so buyers pile in (a spike in lines) and every one of those sales loses money (negative margin). A real team would catch this from exactly this view, and the Section 5 dashboards and the Section 6 app are built to surface it. The summary is a plain reporting query, a display rather than pipeline logic, which is why it is written in SQL.

# COMMAND ----------

import datetime as dt
gold = f"{catalog}.helios_gold"

# Reporting query (a display, not pipeline logic): the incident product at Ares by date.
incident = spark.sql(f"""
    SELECT d.date,
           COUNT(*)                  AS incident_lines,
           ROUND(SUM(f.margin), 0)   AS incident_margin
    FROM {gold}.fact_order_lines f
    JOIN {gold}.dim_date d ON f.date_key = d.date_key
    WHERE f.warehouse_id = 'DEP-03' AND f.product_id = 'PRD-007'
    GROUP BY d.date
    ORDER BY d.date
""")
display(incident)

rows = incident.collect()
peak = max(rows, key=lambda r: r["incident_lines"])
check("the buying spike peaks on the batch three day (the incident)", dt.date(2257, 3, 3), peak["date"])
loss_day = next(r for r in rows if r["date"] == dt.date(2257, 3, 3))
check_true("the incident sales lose money (negative margin at Ares that day)", loss_day["incident_margin"] < 0)

fact = spark.table(f"{gold}.fact_order_lines")
check_true("the fact grew well past the first batch", fact.count() > 150_000)
check("the fact is still one row per line after five batches",
      fact.count(), fact.select("order_line_id").distinct().count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## What you saw
# MAGIC
# MAGIC You ran the orchestrated medallion Job across all five batches of the simulator, watching it stay correct after every drop: Silver and Gold reconciled to the landed source, the price and tier history and the three facts held as the tables grew, and the in pipeline self check gated every run. You also saw a real incident surface: a propulsion part mispriced below cost at Ares on the batch three day, a buying spike that lost money, exactly the kind of signal the next sections build on. Your Section 3 pipeline is built, orchestrated, and proven across multiple batches.