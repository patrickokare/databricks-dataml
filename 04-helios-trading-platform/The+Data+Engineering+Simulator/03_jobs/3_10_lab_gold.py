# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 3.10: Gold: star schema, point in time pricing, three facts
# MAGIC
# MAGIC ### Ticket: HELIOS-310
# MAGIC
# MAGIC **Context.** Silver is clean and conformed. Gold is the shape the business queries: a star schema with conformed dimensions and three facts, one per order line, one per return, one per stock movement. You build it in the DataFrame and Delta APIs, matching the operation to the table: **overwrite the small dimensions; upsert the large facts** on their business keys. The order line and return facts are valued with a point in time join to the price SCD2, so revenue and margin use the price that was in force when the line was placed.
# MAGIC
# MAGIC **Task.**
# MAGIC 1. Overwrite the conformed dimensions, including the two SCD Type 2 dimensions and a generated `dim_date`.
# MAGIC 2. Upsert `fact_order_lines` (keyed on `order_line_id`), point in time priced, with `gross_amount` and `margin`.
# MAGIC 3. Upsert `fact_returns` and `fact_inventory`.
# MAGIC
# MAGIC **Acceptance criteria** (the self check cells):
# MAGIC - Each reference dimension is one row per key and matches its Silver source; the SCD Type 2 dimensions (price and customer) keep one current row per key.
# MAGIC - `fact_order_lines` has one row per line and one per `order_line_id` (the join did not fan out), every line is priced, and `gross_amount` and `margin` are set.
# MAGIC - `fact_returns` refunds at the original paid price and points at real lines; `fact_inventory` has one row per movement key.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bootstrap
# MAGIC Starts from a clean slate so the lab is deterministic on every run: reset the Bronze, Silver and Gold schemas, clear the landing Volume, and re-land batches one to three. Then it rebuilds the full upstream stack by running the production task notebooks: Bronze, then Silver and the SCD2 dimensions, so you have fresh inputs to build Gold on (returns and price and tier changes are all present by batch three).

# COMMAND ----------

# MAGIC %run ../00_setup/data_generator

# COMMAND ----------

# MAGIC %run ../00_setup/bootstrap_helpers

# COMMAND ----------

from pyspark.sql.functions import (
    col, to_date, date_format, lit, year, month, dayofmonth, dayofweek, explode, sequence, expr,
    coalesce, min as min_, max as max_,
)
from delta.tables import DeltaTable

# Clean slate: reset Bronze and Silver (rebuilt below) and Gold (where you build the star schema), and re-land
# batches one to three, so the lab is deterministic on every run.
catalog = helios_identity()
for layer in ["helios_bronze", "helios_silver", "helios_gold"]:
    reset_schema(catalog, layer)
clear_landing(catalog)
ensure_landing(catalog, up_to_batch=3)

# COMMAND ----------

# MAGIC %run ./job_tasks/01_bronze

# COMMAND ----------

# MAGIC %run ./job_tasks/02_silver

# COMMAND ----------

# MAGIC %run ./job_tasks/03_scd

# COMMAND ----------

# MAGIC %md
# MAGIC ## The upsert helper (from Lab 3.8)
# MAGIC The same generic `upsert` you wrote and tested in Lab 3.8, provided so this lab stands on its own. You use it for the three facts. The dimensions are full snapshots, so those you overwrite.

# COMMAND ----------

def upsert(source, table, keys):
    """Create the target if needed, then MERGE the source into it on the key(s). Idempotent and incremental."""
    keys = [keys] if isinstance(keys, str) else keys
    source.limit(0).write.format("delta").mode("ignore").saveAsTable(table)
    condition = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (DeltaTable.forName(spark, table).alias("t")
        .merge(source.alias("s"), condition)
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Build the conformed dimensions and dim_date
# MAGIC
# MAGIC Build the conformed Gold dimensions, `dim_product`, `dim_customer` (the customer SCD Type 2), `dim_supplier`, `dim_warehouse`, `dim_category`, and a generated `dim_date`, by overwriting each one from Silver. The dimensions, their Silver sources and the full column layout for each are set out in `3_5_gold_approach.md`, in this folder; build each one to the schema there.
# MAGIC
# MAGIC **How.**
# MAGIC - Gold is the star schema the business queries: facts in the middle, dimensions around them.
# MAGIC - Build the five conformed dimensions as straight, typed projections of their Silver tables. `dim_customer` reads from the customer SCD Type 2 (`silver_customer_scd`), so it carries each customer's tier history. The price SCD Type 2 stays in Silver (`silver_price_scd`); the facts price point in time off it, so Gold needs no separate price dimension.
# MAGIC - Build `dim_date` to cover the range of order line and stock movement dates, so any time-based question can join to one shared calendar. Give it a `date_key` that is the date as a `YYYYMMDD` integer (the key the facts join on), plus `day_name` and `is_weekend`.
# MAGIC - These are small and derived straight from Silver, so overwrite each one on every run.
# MAGIC
# MAGIC **Example `dim_date` row:**
# MAGIC
# MAGIC | date_key | date | year | month | day |
# MAGIC |---|---|---|---|---|
# MAGIC | `22570301` | `2257-03-01` | `2257` | `3` | `1` |
# MAGIC
# MAGIC **Definition of done.**
# MAGIC - Each of `dim_product`, `dim_supplier`, `dim_warehouse` and `dim_category` is one row per key and matches its Silver source row for row.
# MAGIC - `dim_customer` is SCD Type 2, with exactly one current row per key.
# MAGIC - `dim_date` has one row per day across the date range, with `date_key` a `YYYYMMDD` integer.
# MAGIC - All seven dimensions are overwritten, so a re-run produces exactly the same result.
# MAGIC
# MAGIC **References.** You may find these helpful:
# MAGIC - `3_5_gold_approach.md` in this folder, the approach doc for this lab: the star schema, the full table schemas, point in time pricing and worked examples
# MAGIC - [Data modeling](https://docs.databricks.com/aws/en/transform/data-modeling)
# MAGIC - [Tutorial: Create and manage Delta Lake tables](https://docs.databricks.com/aws/en/delta/tutorial)
# MAGIC - [Built-in functions](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-functions-builtin.html)

# COMMAND ----------

# Your turn. Overwrite the eight Gold dimensions.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Task 1: Build the conformed dimensions and dim_date (solution)
# MAGIC <details>
# MAGIC <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; overflow:auto;"></pre>
# MAGIC
# MAGIC <script>
# MAGIC var codeB64 = "c2lsdmVyID0gZiJ7Y2F0YWxvZ30uaGVsaW9zX3NpbHZlciIKZ29sZCA9IGYie2NhdGFsb2d9LmhlbGlvc19nb2xkIgoKIyBFYWNoIGRpbWVuc2lvbiBpcyBhIHR5cGVkIHByb2plY3Rpb24gb2YgaXRzIFNpbHZlciB0YWJsZSwgb3ZlcndyaXR0ZW4gc28gYSByZS1ydW4gcmVwcm9kdWNlcyBpdCBleGFjdGx5LgpkaW1lbnNpb25zID0gWwogICAgKCJzaWx2ZXJfcHJvZHVjdHMiLCAiZGltX3Byb2R1Y3QiLAogICAgIFsicHJvZHVjdF9pZCIsICJza3UiLCAicHJvZHVjdF9uYW1lIiwgImNhdGVnb3J5X2lkIiwgInN1cHBsaWVyX2lkIiwgIm1hc3Nfa2ciLCAiaGF6YXJkX2NsYXNzIiwgImFjdGl2ZSJdKSwKICAgICgic2lsdmVyX2N1c3RvbWVyX3NjZCIsICJkaW1fY3VzdG9tZXIiLAogICAgIFsiY3VzdG9tZXJfc2siLCAiY3VzdG9tZXJfaWQiLCAiY3VzdG9tZXJfbmFtZSIsICJjdXN0b21lcl90eXBlIiwgInRpZXIiLCAiaG9tZV93YXJlaG91c2VfaWQiLAogICAgICAic2lnbnVwX2RhdGUiLCAiZWZmZWN0aXZlX2Zyb20iLCAiZWZmZWN0aXZlX3RvIiwgImlzX2N1cnJlbnQiXSksCiAgICAoInNpbHZlcl9zdXBwbGllcnMiLCAiZGltX3N1cHBsaWVyIiwgWyJzdXBwbGllcl9pZCIsICJzdXBwbGllcl9uYW1lIiwgImhvbWVfcmVnaW9uIiwgImFjdGl2ZSJdKSwKICAgICgic2lsdmVyX3dhcmVob3VzZXMiLCAiZGltX3dhcmVob3VzZSIsCiAgICAgWyJ3YXJlaG91c2VfaWQiLCAid2FyZWhvdXNlX25hbWUiLCAiYm9keSIsICJyZWdpb24iLCAidXBsaW5rX3JlbGlhYmlsaXR5Il0pLAogICAgKCJzaWx2ZXJfY2F0ZWdvcmllcyIsICJkaW1fY2F0ZWdvcnkiLCBbImNhdGVnb3J5X2lkIiwgImNhdGVnb3J5X25hbWUiLCAiZGVwYXJ0bWVudCJdKSwKXQpmb3Igc3JjLCBkc3QsIGNvbHMgaW4gZGltZW5zaW9uczoKICAgIChzcGFyay5yZWFkLnRhYmxlKGYie3NpbHZlcn0ue3NyY30iKS5zZWxlY3QoKmNvbHMpCiAgICAgICAgLndyaXRlLmZvcm1hdCgiZGVsdGEiKS5tb2RlKCJvdmVyd3JpdGUiKS5vcHRpb24oIm92ZXJ3cml0ZVNjaGVtYSIsICJ0cnVlIikuc2F2ZUFzVGFibGUoZiJ7Z29sZH0ue2RzdH0iKSkKCiMgZGltX2RhdGU6IG9uZSByb3cgcGVyIGRheSBhY3Jvc3MgdGhlIG9yZGVyIGxpbmUgYW5kIHN0b2NrIG1vdmVtZW50IGRhdGUgcmFuZ2UsIHdpdGggYSBZWVlZTU1ERCBpbnRlZ2VyIGtleS4KZGF0ZXMgPSAoCiAgICBzcGFyay5yZWFkLnRhYmxlKGYie3NpbHZlcn0uc2lsdmVyX29yZGVyX2xpbmVzIikuc2VsZWN0KHRvX2RhdGUoImxpbmVfdHMiKS5hbGlhcygiZCIpKQogICAgLnVuaW9uKHNwYXJrLnJlYWQudGFibGUoZiJ7c2lsdmVyfS5zaWx2ZXJfaW52ZW50b3J5Iikuc2VsZWN0KHRvX2RhdGUoIm1vdmVtZW50X3RzIikuYWxpYXMoImQiKSkpCikKc3BhbiA9IGRhdGVzLnNlbGVjdChtaW5fKCJkIikuYWxpYXMoImZyb21fZGF0ZSIpLCBtYXhfKCJkIikuYWxpYXMoInRvX2RhdGUiKSkKZGltX2RhdGUgPSAoCiAgICBzcGFuLnNlbGVjdChleHBsb2RlKHNlcXVlbmNlKGNvbCgiZnJvbV9kYXRlIiksIGNvbCgidG9fZGF0ZSIpLCBleHByKCJJTlRFUlZBTCAxIERBWSIpKSkuYWxpYXMoImRhdGUiKSkKICAgIC5zZWxlY3QoZGF0ZV9mb3JtYXQoImRhdGUiLCAieXl5eU1NZGQiKS5jYXN0KCJpbnQiKS5hbGlhcygiZGF0ZV9rZXkiKSwgY29sKCJkYXRlIiksCiAgICAgICAgICAgIHllYXIoImRhdGUiKS5hbGlhcygieWVhciIpLCBtb250aCgiZGF0ZSIpLmFsaWFzKCJtb250aCIpLCBkYXlvZm1vbnRoKCJkYXRlIikuYWxpYXMoImRheSIpLAogICAgICAgICAgICBkYXRlX2Zvcm1hdCgiZGF0ZSIsICJFRUVFIikuYWxpYXMoImRheV9uYW1lIiksIGRheW9md2VlaygiZGF0ZSIpLmlzaW4oMSwgNykuYWxpYXMoImlzX3dlZWtlbmQiKSkpCmRpbV9kYXRlLndyaXRlLmZvcm1hdCgiZGVsdGEiKS5tb2RlKCJvdmVyd3JpdGUiKS5vcHRpb24oIm92ZXJ3cml0ZVNjaGVtYSIsICJ0cnVlIikuc2F2ZUFzVGFibGUoZiJ7Z29sZH0uZGltX2RhdGUiKQ==";
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
# MAGIC ### Self check: Task 1

# COMMAND ----------

gold = f"{catalog}.helios_gold"
silver = f"{catalog}.helios_silver"

# Invariants, not magic counts, so a later batch never makes a correct dimension fail.
for dim, src, key in [("dim_product", "silver_products", "product_id"),
                      ("dim_supplier", "silver_suppliers", "supplier_id"),
                      ("dim_warehouse", "silver_warehouses", "warehouse_id"),
                      ("dim_category", "silver_categories", "category_id")]:
    dimension = spark.table(f"{gold}.{dim}")
    check(f"{dim} has one row per {key}", dimension.count(), dimension.select(key).distinct().count())
    check(f"{dim} matches its Silver source", spark.table(f"{silver}.{src}").count(), dimension.count())

check("silver_price_scd has one current row per product", 0,
      spark.table(f"{silver}.silver_price_scd").filter("is_current").groupBy("product_id").count().filter("count <> 1").count())
check("dim_customer has one current row per customer (SCD Type 2)", 0,
      spark.table(f"{gold}.dim_customer").filter("is_current").groupBy("customer_id").count().filter("count <> 1").count())

dim_date = spark.table(f"{gold}.dim_date")
check("dim_date has one row per date_key", dim_date.count(), dim_date.select("date_key").distinct().count())
check("date_key is typed as an int", "int", dict(dim_date.dtypes)["date_key"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Build fact_order_lines with point in time pricing
# MAGIC
# MAGIC Build `fact_order_lines` at the grain of one order line, valuing each line with the price and cost that were in effect when it was placed, computing `gross_amount` and `margin`, and upserting it on `order_line_id`. This is a bookings grain: it keeps every placed line, including lines whose order was later cancelled or is still in flight. The full `fact_order_lines` schema, every column and its type, is set out in `3_5_gold_approach.md`, in this folder; build to the schema there.
# MAGIC
# MAGIC **How.**
# MAGIC - Build the fact from `silver_order_lines`, carrying `order_id` (a degenerate dimension) and the foreign keys to product, customer and warehouse.
# MAGIC - Cost each line by joining to the price SCD2, but the pricing must be point in time: use the price version whose effective range covers the line's `line_ts`, not whichever version is current now, otherwise a line from last year would be valued at today's price.
# MAGIC - From the matched version take `unit_price`, `unit_cost`, `supplier_id` and `price_sk`, then compute `gross_amount` (`quantity` times `unit_price`) and `margin` (`gross_amount` minus `quantity` times `unit_cost`).
# MAGIC - Set `is_returned` true when the line appears in `silver_returns`.
# MAGIC - Carry the order's current lifecycle status onto the line as `order_status`, joined from `silver_orders` (its current state, one row per `order_id`, so the join cannot fan out). It is NULL when the order has no current record: the change has not arrived yet, or the order was cancelled and purged. This lets a reader net out cancellations later (exclude `order_status = 'CANCELLED'`) without changing the bookings grain.
# MAGIC - This is a big table, so build it with your `upsert` helper on `order_line_id` rather than rebuilding the whole thing each run.
# MAGIC
# MAGIC **Example.** `OL-1-000001` bought `PRD-017` (a propulsion part) on `2257-03-01`, quantity `2`, valued at that date's version (`price_sk` `PRD-017_22570301`, `unit_price` `10184.99`, `unit_cost` `5703.59`), so `gross_amount` is `20369.98` and `margin` is `8962.80`:
# MAGIC
# MAGIC | order_line_id | product_id | date_key | price_sk | unit_price | quantity | gross_amount | margin |
# MAGIC |---|---|---|---|---|---|---|---|
# MAGIC | `OL-1-000001` | `PRD-017` | `22570301` | `PRD-017_22570301` | `10184.99` | `2` | `20369.98` | `8962.80` |
# MAGIC
# MAGIC **Definition of done.**
# MAGIC - `fact_order_lines` has one row per line, it matches `silver_order_lines`, and one per `order_line_id`, so the join did not fan out (row count equals distinct `order_line_id`).
# MAGIC - Every line has a non-null `price_sk`, `unit_price` and `unit_cost` (every product has a price from the base date).
# MAGIC - `gross_amount` equals `quantity` times `unit_price`, and `margin` equals `gross_amount` minus `quantity` times `unit_cost`.
# MAGIC - `is_returned` is true exactly for the lines that have a return.
# MAGIC - `order_status` is present and is either NULL or one of the eight lifecycle statuses.
# MAGIC - Built by `upsert` on `order_line_id`.
# MAGIC
# MAGIC **References.** You may find these helpful:
# MAGIC - `3_5_gold_approach.md` in this folder, the approach doc for this lab: the star schema, the full table schemas, point in time pricing and worked examples
# MAGIC - [Upsert into a Delta Lake table using merge](https://docs.databricks.com/aws/en/delta/merge)
# MAGIC - [Point-in-time feature joins](https://docs.databricks.com/aws/en/machine-learning/feature-store/time-series)
# MAGIC - [Data modeling](https://docs.databricks.com/aws/en/transform/data-modeling)
# MAGIC - [Range join optimization](https://docs.databricks.com/aws/en/optimizations/range-join)

# COMMAND ----------

# Your turn. Build the point in time priced fact and upsert it on order_line_id.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Task 2: Build fact_order_lines with point in time pricing (solution)
# MAGIC <details>
# MAGIC <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; overflow:auto;"></pre>
# MAGIC
# MAGIC <script>
# MAGIC var codeB64 = "bGluZXMgPSBzcGFyay5yZWFkLnRhYmxlKGYie3NpbHZlcn0uc2lsdmVyX29yZGVyX2xpbmVzIikuYWxpYXMoImwiKQpwcmljZXMgPSBzcGFyay5yZWFkLnRhYmxlKGYie3NpbHZlcn0uc2lsdmVyX3ByaWNlX3NjZCIpLmFsaWFzKCJwIikKIyBvcmRlcl9zdGF0dXM6IHRoZSBvcmRlcidzIGN1cnJlbnQgbGlmZWN5Y2xlIHN0YXR1cyAoYSBkZWdlbmVyYXRlIGF0dHJpYnV0ZSksIGZyb20gdGhlIGN1cnJlbnQtc3RhdGUKIyBzaWx2ZXJfb3JkZXJzLCBsZWZ0IGpvaW5lZCBvbiBvcmRlcl9pZCAob25lIHJvdyBwZXIgb3JkZXIgdGhlcmUsIHNvIG5vIGZhbiBvdXQpLiBOVUxMIHdoZW4gdGhlIG9yZGVyIGhhcyBubwojIGN1cnJlbnQgcmVjb3JkOiBpdHMgQ0RDIGNoYW5nZSBoYXMgbm90IGFycml2ZWQgeWV0LCBvciBpdCB3YXMgYSBwdXJnZWQgKHNvZnQgZGVsZXRlZCkgY2FuY2VsbGF0aW9uLgpvcmRlcnMgPSBzcGFyay5yZWFkLnRhYmxlKGYie3NpbHZlcn0uc2lsdmVyX29yZGVycyIpLnNlbGVjdCgKICAgIGNvbCgib3JkZXJfaWQiKS5hbGlhcygiX29yZF9pZCIpLCBjb2woInN0YXR1cyIpLmFsaWFzKCJvcmRlcl9zdGF0dXMiKSkKCmZhY3QgPSAoCiAgICBsaW5lcy5qb2luKAogICAgICAgIHByaWNlcywKICAgICAgICAoY29sKCJsLnByb2R1Y3RfaWQiKSA9PSBjb2woInAucHJvZHVjdF9pZCIpKQogICAgICAgICYgKHRvX2RhdGUoY29sKCJsLmxpbmVfdHMiKSkgPj0gY29sKCJwLmVmZmVjdGl2ZV9mcm9tIikpCiAgICAgICAgJiAoY29sKCJwLmVmZmVjdGl2ZV90byIpLmlzTnVsbCgpIHwgKHRvX2RhdGUoY29sKCJsLmxpbmVfdHMiKSkgPCBjb2woInAuZWZmZWN0aXZlX3RvIikpKSwKICAgICAgICAibGVmdCIpCiAgICAuam9pbihvcmRlcnMsIGNvbCgibC5vcmRlcl9pZCIpID09IGNvbCgiX29yZF9pZCIpLCAibGVmdCIpCiAgICAuc2VsZWN0KGNvbCgibC5vcmRlcl9saW5lX2lkIiksIGNvbCgibC5vcmRlcl9pZCIpLCBjb2woImwucHJvZHVjdF9pZCIpLCBjb2woImwuY3VzdG9tZXJfaWQiKSwKICAgICAgICAgICAgY29sKCJsLndhcmVob3VzZV9pZCIpLCBjb2woInAuc3VwcGxpZXJfaWQiKSwKICAgICAgICAgICAgZGF0ZV9mb3JtYXQoY29sKCJsLmxpbmVfdHMiKSwgInl5eXlNTWRkIikuY2FzdCgiaW50IikuYWxpYXMoImRhdGVfa2V5IiksCiAgICAgICAgICAgIGNvbCgicC5wcmljZV9zayIpLCBjb2woImwucXVhbnRpdHkiKSwgY29sKCJwLnVuaXRfcHJpY2UiKSwgY29sKCJwLnVuaXRfY29zdCIpLAogICAgICAgICAgICAoY29sKCJsLnF1YW50aXR5IikgKiBjb2woInAudW5pdF9wcmljZSIpKS5jYXN0KCJkZWNpbWFsKDE0LDIpIikuYWxpYXMoImdyb3NzX2Ftb3VudCIpLAogICAgICAgICAgICAoY29sKCJsLnF1YW50aXR5IikgKiAoY29sKCJwLnVuaXRfcHJpY2UiKSAtIGNvbCgicC51bml0X2Nvc3QiKSkpLmNhc3QoImRlY2ltYWwoMTQsMikiKS5hbGlhcygibWFyZ2luIiksCiAgICAgICAgICAgIGNvbCgib3JkZXJfc3RhdHVzIikpCikKCiMgaXNfcmV0dXJuZWQ6IHRoZSBsaW5lIGFwcGVhcnMgaW4gc2lsdmVyX3JldHVybnMuCnJldHVybmVkID0gc3BhcmsucmVhZC50YWJsZShmIntzaWx2ZXJ9LnNpbHZlcl9yZXR1cm5zIikuc2VsZWN0KCJvcmRlcl9saW5lX2lkIikuZGlzdGluY3QoKS53aXRoQ29sdW1uKCJfcmV0IiwgbGl0KFRydWUpKQpmYWN0ID0gZmFjdC5qb2luKHJldHVybmVkLCAib3JkZXJfbGluZV9pZCIsICJsZWZ0Iikud2l0aENvbHVtbigiaXNfcmV0dXJuZWQiLCBjb2FsZXNjZShjb2woIl9yZXQiKSwgbGl0KEZhbHNlKSkpLmRyb3AoIl9yZXQiKQoKdXBzZXJ0KGZhY3QsIGYie2dvbGR9LmZhY3Rfb3JkZXJfbGluZXMiLCAib3JkZXJfbGluZV9pZCIp";
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
# MAGIC ### Self check: Task 2

# COMMAND ----------

silver = f"{catalog}.helios_silver"
fact = spark.table(f"{gold}.fact_order_lines")
lines = spark.table(f"{silver}.silver_order_lines")

check("fact has one row per clean line", lines.count(), fact.count())
check("the point in time join did not fan out", fact.count(), fact.select("order_line_id").distinct().count())
check("every line is priced", 0, fact.filter("price_sk IS NULL").count())
check("gross_amount equals quantity times unit_price", 0,
      fact.filter("gross_amount <> quantity * unit_price").count())
check("margin equals gross_amount minus quantity times unit_cost", 0,
      fact.filter("margin <> gross_amount - quantity * unit_cost").count())
check("order_status is null or a valid lifecycle status", 0,
      fact.filter("order_status IS NOT NULL AND order_status NOT IN "
                 "('PLACED','PAID','PICKED','SHIPPED','DELIVERED','BACKORDERED','CANCELLED','RETURNED')").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Build fact_returns and fact_inventory
# MAGIC
# MAGIC Build the other two facts: `fact_returns` (one row per return, refunded at the original paid price) and `fact_inventory` (one row per stock movement), upserting each on its business key. The `fact_returns` and `fact_inventory` schemas, every column and its type, are set out in `3_5_gold_approach.md`, in this folder; build to the schema there.
# MAGIC
# MAGIC **How.**
# MAGIC - `fact_returns`: join each return to its original order line, then point in time join that order line to the price SCD2 (by the order line's `line_ts`), so the refund uses the price the customer actually paid, not today's price. Compute `refund_amount` as `quantity_returned` times that original `unit_price`. Carry the product, customer and warehouse from the order line, and a `date_key` from the return date. Upsert on `return_id`.
# MAGIC - `fact_inventory`: a straight projection of `silver_inventory` (the signed `delta` and the running `on_hand`), with a `date_key` from `movement_ts`. Upsert on the movement key (`warehouse_id`, `product_id`, `movement_ts`).
# MAGIC
# MAGIC **Example.** A return of one unit of an order line that was bought at `10184.99` refunds `10184.99`, even if that product's price has since changed. A `fact_inventory` row for the Ares incident product on the stockout day shows `on_hand` reaching `0`.
# MAGIC
# MAGIC **Definition of done.**
# MAGIC - `fact_returns` has one row per `return_id`, every row points at a real order line, and `refund_amount` equals `quantity_returned` times the order line's original `unit_price`.
# MAGIC - `fact_inventory` has one row per (`warehouse_id`, `product_id`, `movement_ts`) and a non-null `date_key` on every row.
# MAGIC - Both are built by `upsert`.
# MAGIC
# MAGIC **References.** You may find these helpful:
# MAGIC - `3_5_gold_approach.md` in this folder, the approach doc for this lab: the star schema, the full table schemas, point in time pricing and worked examples
# MAGIC - [Upsert into a Delta Lake table using merge](https://docs.databricks.com/aws/en/delta/merge)
# MAGIC - [Point-in-time feature joins](https://docs.databricks.com/aws/en/machine-learning/feature-store/time-series)
# MAGIC - [Data modeling](https://docs.databricks.com/aws/en/transform/data-modeling)

# COMMAND ----------

# Your turn. Build fact_returns (refund at original price) and fact_inventory.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Task 3: Build fact_returns and fact_inventory (solution)
# MAGIC <details>
# MAGIC <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; overflow:auto;"></pre>
# MAGIC
# MAGIC <script>
# MAGIC var codeB64 = "IyBmYWN0X3JldHVybnM6IHJlZnVuZCBhdCB0aGUgT1JJR0lOQUwgcGFpZCBwcmljZSAocG9pbnQgaW4gdGltZSBhdCB0aGUgbGluZSkuCnJldHMgPSBzcGFyay5yZWFkLnRhYmxlKGYie3NpbHZlcn0uc2lsdmVyX3JldHVybnMiKS5hbGlhcygiciIpCmxpbmUyID0gc3BhcmsucmVhZC50YWJsZShmIntzaWx2ZXJ9LnNpbHZlcl9vcmRlcl9saW5lcyIpLmFsaWFzKCJsIikKcHJpY2UyID0gc3BhcmsucmVhZC50YWJsZShmIntzaWx2ZXJ9LnNpbHZlcl9wcmljZV9zY2QiKS5hbGlhcygicCIpCmZhY3RfciA9ICgKICAgIHJldHMuam9pbihsaW5lMiwgY29sKCJyLm9yZGVyX2xpbmVfaWQiKSA9PSBjb2woImwub3JkZXJfbGluZV9pZCIpLCAiaW5uZXIiKQogICAgLmpvaW4ocHJpY2UyLAogICAgICAgICAgKGNvbCgibC5wcm9kdWN0X2lkIikgPT0gY29sKCJwLnByb2R1Y3RfaWQiKSkKICAgICAgICAgICYgKHRvX2RhdGUoY29sKCJsLmxpbmVfdHMiKSkgPj0gY29sKCJwLmVmZmVjdGl2ZV9mcm9tIikpCiAgICAgICAgICAmIChjb2woInAuZWZmZWN0aXZlX3RvIikuaXNOdWxsKCkgfCAodG9fZGF0ZShjb2woImwubGluZV90cyIpKSA8IGNvbCgicC5lZmZlY3RpdmVfdG8iKSkpLAogICAgICAgICAgImxlZnQiKQogICAgLnNlbGVjdChjb2woInIucmV0dXJuX2lkIiksIGNvbCgici5vcmRlcl9saW5lX2lkIiksIGNvbCgibC5wcm9kdWN0X2lkIiksIGNvbCgibC5jdXN0b21lcl9pZCIpLAogICAgICAgICAgICBjb2woImwud2FyZWhvdXNlX2lkIiksCiAgICAgICAgICAgIGRhdGVfZm9ybWF0KGNvbCgici5yZXR1cm5fdHMiKSwgInl5eXlNTWRkIikuY2FzdCgiaW50IikuYWxpYXMoImRhdGVfa2V5IiksCiAgICAgICAgICAgIGNvbCgici5xdWFudGl0eSIpLmFsaWFzKCJxdWFudGl0eV9yZXR1cm5lZCIpLAogICAgICAgICAgICAoY29sKCJyLnF1YW50aXR5IikgKiBjb2woInAudW5pdF9wcmljZSIpKS5jYXN0KCJkZWNpbWFsKDE0LDIpIikuYWxpYXMoInJlZnVuZF9hbW91bnQiKSkKKQp1cHNlcnQoZmFjdF9yLCBmIntnb2xkfS5mYWN0X3JldHVybnMiLCAicmV0dXJuX2lkIikKCiMgZmFjdF9pbnZlbnRvcnk6IHN0b2NrIG1vdmVtZW50cyBhdCB0aGUgKHdhcmVob3VzZSwgcHJvZHVjdCwgbW92ZW1lbnRfdHMpIGdyYWluLgptb3ZlbWVudHMgPSBzcGFyay5yZWFkLnRhYmxlKGYie3NpbHZlcn0uc2lsdmVyX2ludmVudG9yeSIpLnNlbGVjdCgKICAgICJ3YXJlaG91c2VfaWQiLCAicHJvZHVjdF9pZCIsICJtb3ZlbWVudF90cyIsCiAgICBkYXRlX2Zvcm1hdCgibW92ZW1lbnRfdHMiLCAieXl5eU1NZGQiKS5jYXN0KCJpbnQiKS5hbGlhcygiZGF0ZV9rZXkiKSwgImRlbHRhIiwgIm9uX2hhbmQiKQp1cHNlcnQobW92ZW1lbnRzLCBmIntnb2xkfS5mYWN0X2ludmVudG9yeSIsIFsid2FyZWhvdXNlX2lkIiwgInByb2R1Y3RfaWQiLCAibW92ZW1lbnRfdHMiXSk=";
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

silver = f"{catalog}.helios_silver"
fact_r = spark.table(f"{gold}.fact_returns")
fact_i = spark.table(f"{gold}.fact_inventory")
lines = spark.table(f"{silver}.silver_order_lines")

check("fact_returns has one row per return_id", fact_r.count(), fact_r.select("return_id").distinct().count())
check("every return points at a real order line", 0,
      fact_r.select("order_line_id").join(lines.select("order_line_id"), "order_line_id", "left_anti").count())
check("every return has a refund amount", 0, fact_r.filter("refund_amount IS NULL").count())
check("fact_inventory has one row per movement key", 0,
      fact_i.groupBy("warehouse_id", "product_id", "movement_ts").count().filter("count > 1").count())
check("every inventory movement has a date_key", 0, fact_i.filter("date_key IS NULL").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## What you built
# MAGIC
# MAGIC A Gold star schema in the DataFrame and Delta APIs: conformed dimensions overwritten from Silver (including the price and customer SCD Type 2 dimensions and a date dimension), and three facts upserted on their keys, `fact_order_lines` and `fact_returns` valued point in time against the price SCD2, and `fact_inventory` at the stock movement grain. Overwrite the small dimensions, upsert the event grain facts. This is the canonical Gold that Sections 5 and 6 consume.