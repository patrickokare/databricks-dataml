# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase bootstrap (shared helper)
# MAGIC
# MAGIC Shared setup for Lab 6.6. It runs the Genie bootstrap, which rebuilds the canonical Gold from batches one to five and the Section 5 semantic layer, then builds the curated serving table that Lab 6.5 produces: `depot_ops_summary`, derived from the Section 5 metric views. A lab runs it with a single `%run ../00_setup/lakebase_bootstrap`.
# MAGIC
# MAGIC You do not edit or run this notebook directly; the lab runs it for you. It is idempotent, so it is safe to re run, and it leaves `catalog` and the helper functions in scope.

# COMMAND ----------

# MAGIC %run ./genie_bootstrap

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

print("Serving table ready with Change Data Feed on: depot_ops_summary (6 depots).")