# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 6.5: Serve curated Gold to Lakebase
# MAGIC
# MAGIC Helios has a fast analytical lakehouse, but a depot manager opening an operations console does not want to wait for a warehouse query. In this walkthrough you take a small, curated slice of Gold and sync it into Lakebase, the managed Postgres store on Databricks, where a live app can read it in milliseconds. This is the analytical to operational handoff: the lakehouse stays the system of record, and Lakebase serves a tiny, always fresh copy of exactly what the console needs.
# MAGIC
# MAGIC This is a follow along walkthrough. There is nothing to write and nothing graded. Run each code cell in order, and where a step lives in the workspace, follow the numbered instructions. Along the way you curate a serving table, create a Lakebase project, register its database in Unity Catalog, sync the table, create a safe test branch, and read the synced data back.
# MAGIC
# MAGIC Lakebase is the newest part of the platform, so the exact labels in your workspace may differ a little from the steps below. Under each workspace step there is a scriptable command line equivalent.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: rebuild the canonical Gold and semantic layer
# MAGIC
# MAGIC The bootstrap lands all five batches, rebuilds the medallion, and restores the Section 5 semantic layer (the three reporting views and the three metric views). It is idempotent, so it is safe to run more than once, and it leaves `catalog` and the helper functions in scope. Just run it.

# COMMAND ----------

# MAGIC %run ../00_setup/genie_bootstrap

# COMMAND ----------

print(f"Working in catalog: {catalog}")

# A quick look at the semantic layer the serving tables are built from.
display(spark.sql(f"""
  SELECT `Depot`, MEASURE(`Revenue`) AS revenue
  FROM {catalog}.helios_semantic.sales_mv
  GROUP BY `Depot` ORDER BY revenue DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why an operational store
# MAGIC
# MAGIC The Gold star schema and the Section 5 metric views live on the SQL warehouse. That is the right home for dashboards and ad hoc questions, where a query taking a few seconds is fine and the value is in scanning millions of rows. An operations console is a different job. A manager picks a depot and expects the numbers at once, then picks another, again and again. Running a fresh warehouse scan on every click is slow and wasteful.
# MAGIC
# MAGIC Lakebase is a managed, Postgres compatible database built for this pattern: many small, low latency reads, keyed lookups, and an app sitting in front. It scales its compute down to zero when idle, so it is cheap to leave running for a lab. The plan is not to copy all of Gold into it. You sync a small curated slice, the exact rows and columns the console shows, and let the lakehouse remain the heavy analytical store behind it. Syncing curated, aggregated tables rather than raw facts is the rule: Postgres is not the place to aggregate millions of order lines.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Curate the serving table
# MAGIC
# MAGIC You serve one small table, `depot_ops_summary`: one row per depot with the headline numbers a manager scans first, revenue, gross margin rate, orders, cancellation rate, on time rate, backordered orders, stockout events and units shipped. It is built from the Section 5 metric views, so the console reports the same figures as Genie and the dashboards, down to the credit.
# MAGIC
# MAGIC Building it from the metric views keeps one definition of every metric across analytics and serving, and it is created with Change Data Feed enabled so a triggered Lakebase sync can pick up only what changed on each run. Run the cell to create the table in your `helios_semantic` schema.

# COMMAND ----------

# depot_ops_summary: one row per depot, built from the three Section 5 metric views
# so every figure matches Genie and the dashboards. warehouse_id and the depot's
# body come from the depot dimension. Change Data Feed is enabled so the Lakebase
# sync can pick up only the rows that changed on each run.
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.helios_semantic.depot_ops_summary
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
AS
WITH s AS (
  SELECT `Depot` AS depot, MEASURE(`Revenue`) AS revenue, MEASURE(`Gross Margin`) AS gross_margin,
         MEASURE(`Gross Margin Rate`) AS gross_margin_rate, MEASURE(`Units Sold`) AS units_sold
  FROM {catalog}.helios_semantic.sales_mv GROUP BY `Depot`),
o AS (
  SELECT `Depot` AS depot, MEASURE(`Orders`) AS orders, MEASURE(`Cancellation Rate`) AS cancellation_rate,
         MEASURE(`On Time Fulfilment Rate`) AS on_time_rate, MEASURE(`Backordered Orders`) AS backordered_orders
  FROM {catalog}.helios_semantic.orders_mv GROUP BY `Depot`),
i AS (
  SELECT `Depot` AS depot, MEASURE(`Stockout Events`) AS stockout_events, MEASURE(`Units Out`) AS units_out
  FROM {catalog}.helios_semantic.inventory_mv GROUP BY `Depot`)
SELECT w.warehouse_id, s.depot, w.region AS depot_region, w.body AS depot_body,
       CAST(s.revenue AS DECIMAL(18,2))         AS revenue,
       CAST(s.gross_margin AS DECIMAL(18,2))    AS gross_margin,
       CAST(s.gross_margin_rate AS DOUBLE)      AS gross_margin_rate,
       CAST(s.units_sold AS BIGINT)             AS units_sold,
       CAST(o.orders AS BIGINT)                 AS orders,
       CAST(o.cancellation_rate AS DOUBLE)      AS cancellation_rate,
       CAST(o.on_time_rate AS DOUBLE)           AS on_time_rate,
       CAST(o.backordered_orders AS BIGINT)     AS backordered_orders,
       CAST(i.stockout_events AS BIGINT)        AS stockout_events,
       CAST(i.units_out AS BIGINT)              AS units_out
FROM s JOIN o ON s.depot = o.depot JOIN i ON s.depot = i.depot
JOIN {catalog}.helios_gold.dim_warehouse w ON w.warehouse_name = s.depot
""")

# COMMAND ----------

print("depot_ops_summary (one row per depot, Ares margin dragged down by the batch three incident):")
display(spark.table(f"{catalog}.helios_semantic.depot_ops_summary").orderBy("revenue", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Lakebase project (workspace UI)
# MAGIC
# MAGIC Lakebase organises everything under a project, so create one to hold the serving copy.
# MAGIC
# MAGIC 1. Open the apps switcher at the top of the workspace and open the **Lakebase** app
# MAGIC 2. Click **New project**
# MAGIC 3. Give the project a short name, for example `helios-ops`, and select the Postgres version.
# MAGIC 4. Create it and wait until it is ready. This takes a minute or two the first time.
# MAGIC
# MAGIC The project comes with a `production` branch, a default database called `databricks_postgres`, and a compute endpoint that scales to zero when idle, so it is cheap to leave running for a lab. To see the connection details later, open the project, select the `production` branch, and click **Connect**.
# MAGIC
# MAGIC If your workspace does not show Lakebase yet, it may not be enabled on your account. Lakebase is new, and Free Edition is still rolling it out; the rest of this section is a read through in that case.
# MAGIC
# MAGIC Prefer to script it? The command line equivalent, from a terminal with the Databricks CLI:
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the project's database in Unity Catalog
# MAGIC
# MAGIC Registering the Postgres database as a Unity Catalog catalog lets you query the synced data from the lakehouse. This is a one time step.
# MAGIC
# MAGIC 1. Go to the Catalog Explorer.
# MAGIC 2. Click the **plus** button and choose **Create a catalog**.
# MAGIC 3. Name the catalog `helios_ops`.
# MAGIC 4. For the catalog type choose **Lakebase Postgres**, then **Autoscaling**.
# MAGIC 5. Select your project, the `production` branch, and the `databricks_postgres` database.
# MAGIC 6. Click **Create**.
# MAGIC
# MAGIC After this, `helios_ops` behaves like any other catalog, and `helios_ops.public` is the default Postgres schema where synced tables appear.
# MAGIC
# MAGIC Command line equivalent:
# MAGIC
# MAGIC ```
# MAGIC databricks postgres create-catalog helios_ops --json '{"spec": {"postgres_database": "databricks_postgres", "branch": "projects/<PROJECT_ID>/branches/production"}}'
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync the serving table
# MAGIC
# MAGIC A synced table keeps a Postgres copy of a Unity Catalog table up to date through a managed pipeline. On Free Edition you can run one sync pipeline at a time, so you sync the single `depot_ops_summary` table; the console reads everything it needs from it. You sync it in triggered mode, so the pipeline refreshes the copy when you run it or on a schedule, instead of running the whole time like continuous, which would keep drawing serverless compute. Triggered mode reads the source Change Data Feed, which you enabled when you built the table, so each run copies only what changed.
# MAGIC
# MAGIC 1. Click **Catalog** in the sidebar, open `<your_catalog>.helios_semantic.depot_ops_summary`, then click **Create** and choose **Synced table**. The **Create synced table** dialog opens.
# MAGIC 2. **Destination.** Under **Name**, open the catalog and schema picker (it defaults to the source, `<your_catalog> . helios_semantic`) and change it to your **`helios_ops`** catalog and its **`public`** schema, then type `depot_ops_summary` in the table name box.
# MAGIC 3. **Lakebase Postgres Database.** Leave **Database type** on **Autoscaling**, then select your **Project** (`helios-ops`), your **Branch** (`production`), and the **Postgres database** (`databricks_postgres`).
# MAGIC 4. **Synchronization settings.** Confirm the **Primary Key** is **warehouse_id** and leave **Primary Key is unique** ticked. Leave **Embedding Column** empty. Set **Sync mode** to **Triggered** (your workspace may label this **On-demand**): it refreshes on a trigger or a schedule instead of running the whole time like **Continuous**, so it does not drain your serverless quota.
# MAGIC 5. **Pipeline settings.** Leave **Pipeline** on **Create new**. The new pipeline needs somewhere to keep its own metadata (its event log and checkpoints), so set its **catalog** and **schema** to your student catalog `<your_catalog>` and its `helios_semantic` schema. This must be a regular Unity Catalog catalog, not the `helios_ops` Lakebase catalog, because a pipeline cannot write its metadata into a Postgres backed catalog. Leave **Serverless usage policy** on **None**.
# MAGIC 6. Click **Create**. A triggered sync runs the first refresh, then waits until you trigger it again or its schedule fires.
# MAGIC
# MAGIC Free Edition allows one sync pipeline, so if you see a `PIPELINE_TYPE_QUOTA_EXCEEDED` error you already have one running: delete the older synced table (and its pipeline) first. The copy is queryable at `helios_ops.public.depot_ops_summary`, from Lakebase and, because you registered the catalog, from the lakehouse.
# MAGIC
# MAGIC Command line equivalent (swap in your student catalog and project id):
# MAGIC
# MAGIC ```
# MAGIC databricks postgres create-synced-table helios_ops.public.depot_ops_summary --json '{
# MAGIC   "spec": {
# MAGIC     "source_table_full_name": "<your_catalog>.helios_semantic.depot_ops_summary",
# MAGIC     "primary_key_columns": ["warehouse_id"],
# MAGIC     "scheduling_policy": "TRIGGERED",
# MAGIC     "branch": "projects/<PROJECT_ID>/branches/production",
# MAGIC     "postgres_database": "databricks_postgres",
# MAGIC     "create_database_objects_if_missing": true,
# MAGIC     "new_pipeline_spec": {"storage_catalog": "<your_catalog>", "storage_schema": "helios_semantic"}
# MAGIC   }
# MAGIC }'
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Branch for safe testing
# MAGIC
# MAGIC A Lakebase branch is a copy on write clone of your database. It shares storage with its parent until you change something, so you can test a schema change or a heavy query against real data without touching the live serving copy. It is the operational version of the same instinct that made you keep a development pipeline in Section 4.
# MAGIC
# MAGIC 1. Open your project and go to the **Branches** page.
# MAGIC 2. Click **Create branch**.
# MAGIC 3. Name it `test`, keep **Branch data and schema**, and set **Auto-delete** to **After 1 day** so it cleans itself up.
# MAGIC 4. Click **Create**.
# MAGIC
# MAGIC To roll a branch back to its parent or remove it, use the three dot menu next to the branch. **Reset from parent** asks you to confirm before it replaces the branch data; **Delete branch** asks you to type the branch name and then click **Delete**. Branching is optional for the rest of the course; it is here so you have seen it.
# MAGIC
# MAGIC Command line equivalent:
# MAGIC
# MAGIC ```
# MAGIC databricks postgres create-branch projects/<PROJECT_ID> test --json '{"spec": {"source_branch": "projects/<PROJECT_ID>/branches/production", "ttl": "86400s"}}'
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read the synced data back
# MAGIC
# MAGIC Once the sync reports success, the synced table is queryable two ways: as a Postgres table inside Lakebase, and as a Unity Catalog table in the `helios_ops` catalog. The cell below reads it through Unity Catalog to confirm the six depot rows landed. Set `lakebase_catalog` to the catalog name you registered, then run it. If the sync is not finished yet the table will not be found; that is expected, come back after it completes.

# COMMAND ----------

lakebase_catalog = "helios_ops"   # the catalog you registered for your Lakebase database

try:
    display(spark.table(f"{lakebase_catalog}.public.depot_ops_summary").orderBy("revenue", ascending=False))
except Exception as e:
    print("Could not read the synced table yet. Finish the sync above first.")
    print(f"Details: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What you saw
# MAGIC
# MAGIC You took the analytical Gold and served a curated slice of it operationally. You built one small table from the Section 5 metric views, so the serving numbers match Genie and the dashboards, then created a Lakebase project, registered its database in Unity Catalog, and synced the table in as a low latency Postgres copy. You also saw copy on write branching for safe testing. The lakehouse is still the system of record; Lakebase now holds a fast, always current copy of exactly what a depot console needs to read.