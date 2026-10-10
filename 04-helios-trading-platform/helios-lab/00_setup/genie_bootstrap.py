# Databricks notebook source
# MAGIC %md
# MAGIC # Genie bootstrap (shared helper)
# MAGIC
# MAGIC Shared setup for Labs 5.4 and 5.5. It lives in `00_setup` alongside the other helpers; a lab runs it with a single `%run ../00_setup/genie_bootstrap`, so each lab's bootstrap stays one line instead of a long block.
# MAGIC
# MAGIC It rebuilds the canonical Gold from the Section 3 production task notebooks (batches one to five) and then builds the finished Lab 5.3 semantic layer: the three reporting views (`order_lines_vw`, `orders_vw`, `inventory_vw`), the three metric views (`sales_mv`, `orders_mv`, `inventory_mv`), and the Gold comments. It lands all five batches so the consumption labs show the full story including the incident, and it is idempotent, so it is safe to re-run.
# MAGIC
# MAGIC You do not edit or run this notebook directly; the labs run it for you. After a lab runs it, `catalog` and the helper functions are in scope for the rest of that lab.

# COMMAND ----------

# MAGIC %run ./data_generator

# COMMAND ----------

# MAGIC %run ./bootstrap_helpers

# COMMAND ----------

catalog = helios_identity()

# Clean slate, then land batches one to five and rebuild bronze, silver, the SCD2 dimensions and the gold star
# with the Section 3 production task notebooks, so the semantic layer has the full canonical Gold underneath it.
for layer in ["helios_bronze", "helios_silver", "helios_gold"]:
    reset_schema(catalog, layer)
clear_landing(catalog)
ensure_landing(catalog, up_to_batch=5)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.helios_semantic")

# COMMAND ----------

# MAGIC %run ../03_jobs/job_tasks/01_bronze

# COMMAND ----------

# MAGIC %run ../03_jobs/job_tasks/02_silver

# COMMAND ----------

# MAGIC %run ../03_jobs/job_tasks/03_scd

# COMMAND ----------

# MAGIC %run ../03_jobs/job_tasks/04_gold

# COMMAND ----------

# Provided: the finished Lab 5.3 output (the Gold comments, three reporting views and three metric views).
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.helios_semantic")

# Table comments: state each table's grain and purpose in plain business language.
spark.sql(f"COMMENT ON TABLE {catalog}.helios_gold.fact_order_lines IS 'Order line fact, one row per placed order line. A bookings grain: it keeps lines from cancelled and in flight orders, so revenue and units are gross bookings. Point in time priced and costed; the hero fact for revenue and margin. Exclude CANCELLED order_status for net-of-cancellation revenue.'")
spark.sql(f"COMMENT ON TABLE {catalog}.helios_gold.fact_returns    IS 'Return fact, one row per return, refunded at the price the customer originally paid (point in time at the line).'")
spark.sql(f"COMMENT ON TABLE {catalog}.helios_gold.fact_inventory  IS 'Stock movement fact, one row per movement, carrying the signed delta and the running on_hand balance per depot and product.'")
spark.sql(f"COMMENT ON TABLE {catalog}.helios_gold.dim_warehouse   IS 'Depot dimension. Six depots across the solar system, the slicing axis for every dashboard.'")
spark.sql(f"COMMENT ON TABLE {catalog}.helios_gold.dim_product     IS 'Product dimension. 150 ship parts, each in one category and from one supplier.'")
spark.sql(f"COMMENT ON TABLE {catalog}.helios_gold.dim_category    IS 'Category dimension and its department roll-up (for example PROPULSION rolls up to PROPULSION_AND_POWER).'")
spark.sql(f"COMMENT ON TABLE {catalog}.helios_gold.dim_supplier    IS 'Supplier dimension, one row per supplier.'")
spark.sql(f"COMMENT ON TABLE {catalog}.helios_gold.dim_customer    IS 'Customer dimension as SCD Type 2 (tier over time). Filter is_current for the live tier; one row per customer per tier version.'")
spark.sql(f"COMMENT ON TABLE {catalog}.helios_gold.dim_date        IS 'Generated date dimension, keyed by date_key (the yyyyMMdd integer).'")

# Column comments: explain the columns the business slices and measures on, in generic terms.
spark.sql(f"COMMENT ON COLUMN {catalog}.helios_gold.fact_order_lines.gross_amount IS 'Line revenue: quantity times the point in time unit_price, in CREDITS.'")
spark.sql(f"COMMENT ON COLUMN {catalog}.helios_gold.fact_order_lines.margin       IS 'Line gross profit: revenue minus the point in time cost, in CREDITS. Negative when the line sold below cost.'")
spark.sql(f"COMMENT ON COLUMN {catalog}.helios_gold.fact_order_lines.unit_price   IS 'Price per unit in force when the line was placed (point in time), in CREDITS.'")
spark.sql(f"COMMENT ON COLUMN {catalog}.helios_gold.fact_order_lines.order_status IS 'Current lifecycle status of the order this line belongs to: PLACED, PAID, PICKED, SHIPPED, DELIVERED, BACKORDERED, CANCELLED or RETURNED. NULL when the order has no current record (change not yet arrived, or a purged cancellation). Exclude CANCELLED for net-of-cancellation revenue.'")
spark.sql(f"COMMENT ON COLUMN {catalog}.helios_gold.fact_order_lines.unit_cost    IS 'Supplier cost per unit in force when the line was placed (point in time), in CREDITS.'")
spark.sql(f"COMMENT ON COLUMN {catalog}.helios_gold.fact_order_lines.warehouse_id IS 'The depot that fulfilled the line.'")
spark.sql(f"COMMENT ON COLUMN {catalog}.helios_gold.fact_order_lines.product_id   IS 'The product ordered on the line.'")
spark.sql(f"COMMENT ON COLUMN {catalog}.helios_gold.fact_order_lines.is_returned  IS 'True when the line was later returned.'")
spark.sql(f"COMMENT ON COLUMN {catalog}.helios_gold.dim_customer.tier             IS 'Pricing tier: CHARTER (big contracts), TRADE (regular businesses), DRIFTER (walk-ins). Drives pricing.'")
spark.sql(f"COMMENT ON COLUMN {catalog}.helios_gold.dim_warehouse.region          IS 'Solar system region: INNER, BELT or OUTER.'")
spark.sql(f"COMMENT ON COLUMN {catalog}.helios_gold.fact_inventory.on_hand        IS 'Running stock balance after the movement; zero means a stockout.'")

# Step 1. The reporting view: one flat, business-named row per order line. It joins the fact to each of its
# dimensions so every attribute you slice by sits on one row. Returns are aggregated per line first, so a line
# that has a return still stays exactly one row (the join cannot fan out).
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.helios_semantic.order_lines_vw AS
SELECT
  f.order_line_id, f.order_id, f.product_id, f.customer_id, f.warehouse_id, f.supplier_id,
  f.date_key, f.price_sk, f.quantity, f.unit_price, f.unit_cost, f.gross_amount, f.margin, f.is_returned, f.order_status,
  w.warehouse_name AS depot, w.region AS depot_region, w.body AS depot_body,
  c.category_name, c.department,
  p.product_name, p.sku AS product_sku, p.hazard_class,
  s.supplier_name, s.home_region AS supplier_region,
  cu.customer_type, cu.tier AS customer_tier,
  d.date AS order_date,
  COALESCE(r.returned_quantity, 0) AS returned_quantity,
  COALESCE(r.refund_amount, 0)     AS refund_amount
FROM {catalog}.helios_gold.fact_order_lines f
LEFT JOIN {catalog}.helios_gold.dim_product   p  ON f.product_id   = p.product_id
LEFT JOIN {catalog}.helios_gold.dim_category  c  ON p.category_id  = c.category_id
LEFT JOIN {catalog}.helios_gold.dim_supplier  s  ON f.supplier_id  = s.supplier_id
LEFT JOIN {catalog}.helios_gold.dim_warehouse w  ON f.warehouse_id = w.warehouse_id
LEFT JOIN {catalog}.helios_gold.dim_customer  cu ON f.customer_id  = cu.customer_id AND cu.is_current
LEFT JOIN {catalog}.helios_gold.dim_date      d  ON f.date_key     = d.date_key
LEFT JOIN (
  SELECT order_line_id, SUM(quantity_returned) AS returned_quantity, SUM(refund_amount) AS refund_amount
  FROM {catalog}.helios_gold.fact_returns GROUP BY order_line_id
) r ON f.order_line_id = r.order_line_id
""")

# Step 2. The metric view sits on the reporting view. The source is a SELECT query over the view; the
# dimensions are the columns you slice by; the measures are aggregates the engine computes at query time.
# Query a metric view with MEASURE(<measure name>) and GROUP BY ALL.
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.helios_semantic.sales_mv
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
source: "SELECT * FROM {catalog}.helios_semantic.order_lines_vw"
comment: "Helios sales at the order-line grain: revenue, gross margin, units, orders, average order value and returns, all point in time valued. Slice by depot, region, category, department, product, supplier, customer type and tier, and order date. Sold Below Cost isolates loss-making lines. Net Revenue and Net Units Sold exclude cancelled-order lines (order_status = CANCELLED); Order Status slices by lifecycle state."
dimensions:
  - name: Depot
    expr: depot
    comment: "Fulfilling depot name"
  - name: Depot Region
    expr: depot_region
    comment: "INNER, BELT or OUTER"
  - name: Category
    expr: category_name
    comment: "Product category"
  - name: Department
    expr: department
    comment: "Category roll-up for dashboards"
  - name: Product
    expr: product_name
  - name: Product ID
    expr: product_id
    comment: "Product code"
  - name: Supplier
    expr: supplier_name
  - name: Supplier ID
    expr: supplier_id
    comment: "Supplier code"
  - name: Customer Type
    expr: customer_type
    comment: "Segment: FREIGHT_FLEET, MINER, LINER, RESEARCH, PATROL, INDEPENDENT, COLONIAL"
  - name: Customer Tier
    expr: customer_tier
    comment: "Current pricing tier: CHARTER, TRADE or DRIFTER"
  - name: Order Date
    expr: order_date
    comment: "Date the line was placed"
  - name: Sold Below Cost
    expr: margin < 0
    comment: "True when the line sold under cost (negative margin)"
  - name: Is Returned
    expr: is_returned
  - name: Order Status
    expr: order_status
    comment: "Current lifecycle status of the line's order; NULL if purged or not yet arrived"
measures:
  - name: Revenue
    expr: SUM(gross_amount)
    comment: "Sum of gross_amount (quantity times point in time unit_price), in CREDITS"
  - name: Gross Margin
    expr: SUM(margin)
    comment: "Revenue minus point in time cost, in CREDITS"
  - name: Gross Margin Rate
    expr: SUM(margin) / SUM(gross_amount)
    comment: "Gross Margin divided by Revenue"
  - name: Units Sold
    expr: SUM(quantity)
  - name: Order Lines
    expr: COUNT(1)
  - name: Orders
    expr: COUNT(DISTINCT order_id)
  - name: Average Order Value
    expr: SUM(gross_amount) / COUNT(DISTINCT order_id)
    comment: "Revenue divided by distinct orders, in CREDITS"
  - name: Average Unit Price
    expr: SUM(gross_amount) / SUM(quantity)
  - name: Returned Units
    expr: SUM(returned_quantity)
  - name: Refund Amount
    expr: SUM(refund_amount)
    comment: "Total refunded, valued at the original paid price, in CREDITS"
  - name: Return Rate
    expr: SUM(returned_quantity) / SUM(quantity)
    comment: "Returned units divided by sold units"
  - name: Net Revenue
    expr: SUM(CASE WHEN order_status = 'CANCELLED' THEN 0 ELSE gross_amount END)
    comment: "Revenue excluding cancelled-order lines (gross bookings minus visible cancellations), in CREDITS"
  - name: Net Units Sold
    expr: SUM(CASE WHEN order_status = 'CANCELLED' THEN 0 ELSE quantity END)
    comment: "Units excluding cancelled-order lines"
$$
""")

# Step 1. The reporting view: one row per order (its current state), plus whether it was ever BACKORDERED.
# silver_orders keeps only the current status, so the ever_backordered flag is rebuilt from the raw change
# feed in bronze (MAX over the order's history). Depot and current customer attributes are joined on for slicing.
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.helios_semantic.orders_vw AS
SELECT
  o.order_id, o.customer_id, o.warehouse_id, o.channel, o.order_ts, o.status,
  COALESCE(b.ever_backordered, false) AS ever_backordered,
  to_date(o.order_ts) AS order_date,
  w.warehouse_name AS depot, w.region AS depot_region,
  cu.customer_type, cu.tier AS customer_tier
FROM {catalog}.helios_silver.silver_orders o
LEFT JOIN (
  SELECT order_id, MAX(status = 'BACKORDERED') AS ever_backordered
  FROM {catalog}.helios_bronze.bronze_orders GROUP BY order_id
) b ON o.order_id = b.order_id
LEFT JOIN {catalog}.helios_gold.dim_warehouse w  ON o.warehouse_id = w.warehouse_id
LEFT JOIN {catalog}.helios_gold.dim_customer  cu ON o.customer_id  = cu.customer_id AND cu.is_current
""")

# Step 2. The metric view. The rate measures use FILTER (WHERE ...), which counts only the rows that match the
# condition, so Cancellation Rate is cancelled orders over all orders and On Time Fulfilment Rate is the share
# that reached DELIVERED without ever being BACKORDERED.
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.helios_semantic.orders_mv
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
source: "SELECT * FROM {catalog}.helios_semantic.orders_vw"
comment: "Helios order lifecycle: order volume, cancellation rate and on-time fulfilment. Grain is one order (current state). On-time means the order reached DELIVERED without ever being BACKORDERED. Slice by depot, channel, status, customer type and tier, and order date."
dimensions:
  - name: Depot
    expr: depot
  - name: Depot Region
    expr: depot_region
  - name: Channel
    expr: channel
    comment: "How the order was placed: API, CONSOLE or COUNTER"
  - name: Order Status
    expr: status
    comment: "Current status: PLACED, PAID, PICKED, SHIPPED, DELIVERED, BACKORDERED, CANCELLED, RETURNED"
  - name: Customer Type
    expr: customer_type
  - name: Customer Tier
    expr: customer_tier
  - name: Ever Backordered
    expr: ever_backordered
    comment: "True if the order passed through BACKORDERED at any point in its history"
  - name: Order Date
    expr: order_date
measures:
  - name: Orders
    expr: COUNT(1)
  - name: Cancelled Orders
    expr: COUNT(1) FILTER (WHERE status = 'CANCELLED')
  - name: Cancellation Rate
    expr: COUNT(1) FILTER (WHERE status = 'CANCELLED') / COUNT(1)
    comment: "Cancelled orders divided by all orders"
  - name: Delivered Orders
    expr: COUNT(1) FILTER (WHERE status = 'DELIVERED')
  - name: On Time Fulfilments
    expr: COUNT(1) FILTER (WHERE status = 'DELIVERED' AND NOT ever_backordered)
  - name: On Time Fulfilment Rate
    expr: COUNT(1) FILTER (WHERE status = 'DELIVERED' AND NOT ever_backordered) / COUNT(1)
    comment: "Share of orders that reached DELIVERED without a BACKORDERED step"
  - name: Backordered Orders
    expr: COUNT(1) FILTER (WHERE ever_backordered)
$$
""")

# Step 1. The reporting view: one row per stock movement, with depot and product attributes joined on.
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.helios_semantic.inventory_vw AS
SELECT
  i.warehouse_id, i.product_id, i.movement_ts, i.date_key, i.delta, i.on_hand,
  w.warehouse_name AS depot, w.region AS depot_region,
  p.product_name, c.category_name, c.department, d.date AS movement_date
FROM {catalog}.helios_gold.fact_inventory i
LEFT JOIN {catalog}.helios_gold.dim_warehouse w ON i.warehouse_id = w.warehouse_id
LEFT JOIN {catalog}.helios_gold.dim_product   p ON i.product_id   = p.product_id
LEFT JOIN {catalog}.helios_gold.dim_category  c ON p.category_id  = c.category_id
LEFT JOIN {catalog}.helios_gold.dim_date      d ON i.date_key     = d.date_key
""")

# Step 2. The metric view. Units Out and Units In split the signed delta with a CASE; Stockout Events counts the
# movements where on_hand hit zero; Min On Hand is the lowest balance reached.
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.helios_semantic.inventory_mv
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
source: "SELECT * FROM {catalog}.helios_semantic.inventory_vw"
comment: "Helios stock movements at the movement grain: units in and out, net change and stockouts (on_hand reaching zero). Slice by depot, product, category, department and date. Min On Hand reveals stockouts."
dimensions:
  - name: Depot
    expr: depot
  - name: Depot Region
    expr: depot_region
  - name: Product
    expr: product_name
  - name: Product ID
    expr: product_id
  - name: Category
    expr: category_name
  - name: Department
    expr: department
  - name: Movement Date
    expr: movement_date
  - name: Is Stockout
    expr: on_hand = 0
    comment: "True when on_hand reached zero (a stockout)"
measures:
  - name: Movements
    expr: COUNT(1)
  - name: Stockout Events
    expr: COUNT(1) FILTER (WHERE on_hand = 0)
  - name: Min On Hand
    expr: MIN(on_hand)
  - name: Units Out
    expr: SUM(CASE WHEN delta < 0 THEN -delta ELSE 0 END)
  - name: Units In
    expr: SUM(CASE WHEN delta > 0 THEN delta ELSE 0 END)
  - name: Net Stock Change
    expr: SUM(delta)
$$
""")

print("Semantic layer ready (Gold commented, views and metric views built).")