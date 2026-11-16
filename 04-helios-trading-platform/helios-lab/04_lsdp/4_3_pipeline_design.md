# Helios Trading Corporation: Pipeline Design

The design for the declarative rebuild. Same contract and same target tables as the Databricks Jobs build, this time built by one Lakeflow pipeline. One row per table below, with its materialization, its quality rules and the details that decide its build. The concepts are in `4_2_lsdp_concepts.md`.

One pipeline builds all three layers, into the same schemas the Jobs build writes, `helios_bronze`, `helios_silver` and `helios_gold`. The source is one notebook per layer; the engine reads them together and works out the build order from the table references. The catalog is passed in through the pipeline configuration, never hardcoded.

---

## Bronze

One streaming table per feed, ingested from the landing Volume with Auto Loader. Schema evolution on, unexpected values rescued into `_rescued_data`, every row stamped with `_ingest_ts` and `_source_file`.

| Table | Type | Format |
|---|---|---|
| bronze_order_lines | Streaming table | JSON |
| bronze_orders | Streaming table | JSON |
| bronze_returns | Streaming table | JSON |
| bronze_inventory | Streaming table | JSON |
| bronze_price_list | Streaming table | CSV |
| bronze_customers | Streaming table | Parquet |
| bronze_products | Streaming table | Parquet |
| bronze_categories | Streaming table | Parquet |
| bronze_suppliers | Streaming table | JSON |
| bronze_warehouses | Streaming table | JSON |

---

## Silver

| Table | Type | Source | Key | Expectations | Notes |
|---|---|---|---|---|---|
| silver_order_lines | Streaming table | bronze_order_lines | order_line_id | order_line_id present (fail), quantity > 0 (drop), product present (drop) | typed, deduped within a 48 hour watermark, clustered on order_line_id |
| silver_order_lines_quarantine | Streaming table | bronze_order_lines | order_line_id | | the rows the drop rules rejected |
| silver_orders | Streaming table + AUTO CDC | bronze_orders | order_id | | current state (SCD 1), sequenced by change_seq, DELETE applied as a delete, op column dropped |
| silver_returns | Streaming table | bronze_returns | return_id | return_id present (fail), valid reason (warn) | kept only if it points at a real order line and is dated after it |
| silver_returns_quarantine | Streaming table | bronze_returns | return_id | | orphan returns, and returns dated on or before their line |
| silver_inventory | Streaming table | bronze_inventory | warehouse_id, product_id, movement_ts | | typed stock movements, append |
| silver_price_versions | Streaming table + AUTO CDC | bronze_price_list | product_id | | SCD 2, sequenced by effective_from, engine adds __START_AT and __END_AT |
| silver_price_scd | Materialized view | silver_price_versions | price_sk | | effective_from, effective_to and is_current derived from the engine's columns |
| silver_customer_scd | Streaming table + AUTO CDC from snapshots | daily customer snapshots | customer_id | | SCD 2, a new version whenever a customer's attributes change |
| silver_products | Materialized view | bronze_products | product_id | | typed current copy |
| silver_categories | Materialized view | bronze_categories | category_id | | typed current copy |
| silver_suppliers | Materialized view | bronze_suppliers | supplier_id | | typed current copy |
| silver_warehouses | Materialized view | bronze_warehouses | warehouse_id | | typed current copy |

Drop never deletes; a second flow writes the rejected rows to the matching quarantine table. The typed order and price change feeds are temporary views (`orders_cdc_clean`, `price_cdc_clean`), published nowhere.

### Silver schemas

| Table | Columns |
|---|---|
| silver_order_lines | order_line_id string, order_id string, product_id string, customer_id string, warehouse_id string, quantity int, line_ts timestamp |
| silver_order_lines_quarantine | identical to silver_order_lines |
| silver_orders | order_id string, customer_id string, warehouse_id string, channel string, order_ts timestamp, status string, change_seq long, change_ts timestamp |
| silver_returns | return_id string, order_line_id string, return_ts timestamp, quantity int, reason string |
| silver_returns_quarantine | identical to silver_returns |
| silver_inventory | warehouse_id string, product_id string, movement_ts timestamp, delta int, on_hand int |
| silver_price_versions | product_id string, supplier_id string, unit_price decimal(12,2), unit_cost decimal(12,2), currency string, effective_from date, plus the engine's __START_AT and __END_AT |
| silver_price_scd | price_sk string, product_id string, supplier_id string, unit_price decimal(12,2), unit_cost decimal(12,2), currency string, effective_from date, effective_to date, is_current boolean |
| silver_customer_scd | customer_id string, customer_name string, customer_type string, tier string, home_warehouse_id string, signup_date date, plus the engine's __START_AT and __END_AT |
| silver_products | product_id string, sku string, product_name string, category_id string, supplier_id string, mass_kg double, hazard_class string, active boolean |
| silver_categories | category_id string, category_name string, department string |
| silver_suppliers | supplier_id string, supplier_name string, home_region string, active boolean |
| silver_warehouses | warehouse_id string, warehouse_name string, body string, region string, uplink_reliability double |

---

## Gold

| Table | Type | Source | Key | Notes |
|---|---|---|---|---|
| dim_product | Materialized view | silver_products | product_id | conformed |
| dim_supplier | Materialized view | silver_suppliers | supplier_id | conformed |
| dim_warehouse | Materialized view | silver_warehouses | warehouse_id | conformed |
| dim_category | Materialized view | silver_categories | category_id | conformed |
| dim_date | Materialized view | generated | date_key | one row per day across the span of the data |
| dim_customer | Materialized view | silver_customer_scd | customer_sk | SCD 2, contract columns derived from the engine's |
| fact_order_lines | Materialized view | silver_order_lines, silver_price_scd, silver_returns, silver_orders | order_line_id | priced at the version in force at line_ts; gross_amount, margin, is_returned, order_status; clustered on order_line_id |
| fact_returns | Materialized view | silver_returns, silver_order_lines, silver_price_scd | return_id | refund at the price the customer originally paid |
| fact_inventory | Materialized view | silver_inventory | warehouse_id, product_id, movement_ts | movement grain |

There is no Gold price dimension. The price SCD 2 is conformed once in Silver as `silver_price_scd` and the facts read it from there.

### Gold schemas

| Table | Columns |
|---|---|
| dim_product | product_id string, sku string, product_name string, category_id string, supplier_id string, mass_kg double, hazard_class string, active boolean |
| dim_supplier | supplier_id string, supplier_name string, home_region string, active boolean |
| dim_warehouse | warehouse_id string, warehouse_name string, body string, region string, uplink_reliability double |
| dim_category | category_id string, category_name string, department string |
| dim_date | date_key int, date date, year int, month int, day int, day_name string, is_weekend boolean |
| dim_customer | customer_sk string, customer_id string, customer_name string, customer_type string, tier string, home_warehouse_id string, signup_date date, effective_from date, effective_to date, is_current boolean |
| fact_order_lines | order_line_id string, order_id string, product_id string, customer_id string, warehouse_id string, supplier_id string, date_key int, price_sk string, quantity int, unit_price decimal(12,2), unit_cost decimal(12,2), gross_amount decimal(14,2), margin decimal(14,2), is_returned boolean, order_status string |
| fact_returns | return_id string, order_line_id string, product_id string, customer_id string, warehouse_id string, date_key int, quantity_returned int, refund_amount decimal(14,2) |
| fact_inventory | warehouse_id string, product_id string, movement_ts timestamp, date_key int, delta int, on_hand int |
