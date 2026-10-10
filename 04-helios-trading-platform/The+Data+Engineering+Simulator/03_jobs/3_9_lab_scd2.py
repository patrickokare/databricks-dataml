# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 3.9: Two SCD Type 2 dimensions: price and customer
# MAGIC
# MAGIC ### Ticket: HELIOS-309
# MAGIC
# MAGIC **Context.** Two things change over time and must be read as they were at the moment that matters: a product's price and cost, and a customer (whose contract tier moves). Each is a Slowly Changing Dimension Type 2, a new row every time a tracked value changes, with effective dates, so a point in time join can pick the version in force. You build both the same way: a window shapes the version history (each version's effective range and the current flag), then a single Delta MERGE applies it, so each holds the full history even though the bootstrap landed three batches at once.
# MAGIC
# MAGIC **Task.**
# MAGIC 1. Build `silver_price_scd` from the `price_list` versions with a window and a MERGE.
# MAGIC 2. Build `silver_customer_scd`, the customer dimension itself as SCD Type 2, from the daily snapshots the same way.
# MAGIC
# MAGIC **Acceptance criteria** (the self check cells):
# MAGIC - `silver_price_scd` holds every price version with a unique `price_sk`, exactly one current row per product, and contiguous, non overlapping effective ranges. A product whose price changed (for example `PRD-001`) has two versions, and every product is priced from the base date.
# MAGIC - `silver_customer_scd` carries the full customer attributes, exactly one current row per customer, a unique `customer_sk`, and a customer whose tier changed (for example `CUST-0033`) has two versions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bootstrap
# MAGIC Starts from a clean slate so the lab is deterministic on every run: reset the Bronze and Silver schemas, clear the landing Volume, and re-land batches one to three. By batch three a handful of prices have changed and a few customers have been promoted a tier, so both dimensions have real history. Then it rebuilds Bronze by running the production Bronze task notebook, so `bronze_price_list` and `bronze_customers` are present. The bootstrap lands all three batches at once, which is exactly why the build shapes the full history with a window over every landed version rather than treating one snapshot as the current state.

# COMMAND ----------

# MAGIC %run ../00_setup/data_generator

# COMMAND ----------

# MAGIC %run ../00_setup/bootstrap_helpers

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, date_format, lag, lead
from pyspark.sql import Window
from delta.tables import DeltaTable

# Clean slate: reset Bronze (rebuilt below) and Silver (where the SCD2 dimensions you build live), and re-land
# batches one to three, so price and tier changes are present.
catalog = helios_identity()
for layer in ["helios_bronze", "helios_silver"]:
    reset_schema(catalog, layer)
clear_landing(catalog)
ensure_landing(catalog, up_to_batch=3)

# COMMAND ----------

# MAGIC %run ./job_tasks/01_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: silver_price_scd: build the price history with a window and a MERGE
# MAGIC
# MAGIC Build `silver_price_scd`, an SCD Type 2 of each product's price and cost over time, by reducing the `price_list` feed to its distinct versions, shaping the effective ranges with a window, and applying them with a single Delta MERGE. `3_4_scd2_approach.md`, in this folder, walks this approach with worked examples, including why the source's own effective_to and is_current are recomputed.
# MAGIC
# MAGIC **How.**
# MAGIC - `price_list` is a full effective-dated extract: each row is one price version with its own `effective_from`, and the whole history is re-landed every batch, so Bronze holds overlapping copies. Reduce it to the distinct versions: one row per (`product_id`, `effective_from`).
# MAGIC - Shape the effective range with a window over each product, ordered by `effective_from`: a version's `effective_to` is the next version's start (`lead`), and the version with no next one is current. Give each a surrogate key `price_sk` (`<product_id>_<YYYYMMDD>`).
# MAGIC - Create the `silver_price_scd` table if it does not exist, then `MERGE` the versions in on `price_sk`: update the rows already there (so a version that has just been closed is refreshed) and insert the new ones.
# MAGIC - One window to shape the history, one MERGE to apply it: idempotent, and it touches only what changed.
# MAGIC
# MAGIC **The `price_sk` surrogate key.** For `PRD-001` effective from `2257-03-01` that is `PRD-001_22570301`.
# MAGIC
# MAGIC **Example.** `PRD-001` was `269.99` from `2257-03-01`, then `288.89` from `2257-03-03`. The dimension keeps both, the first closed and the second current:
# MAGIC
# MAGIC | price_sk | unit_price | effective_from | effective_to | is_current |
# MAGIC |---|---|---|---|---|
# MAGIC | `PRD-001_22570301` | `269.99` | `2257-03-01` | `2257-03-03` | `false` |
# MAGIC | `PRD-001_22570303` | `288.89` | `2257-03-03` | `null` | `true` |
# MAGIC
# MAGIC **Definition of done.**
# MAGIC - `silver_price_scd` holds every version with a unique `price_sk` and exactly one current row per product.
# MAGIC - A product whose price changed has more than one version, with contiguous, non overlapping ranges; `PRD-001` has two.
# MAGIC - Every product's earliest version is effective from the base date, so any order line resolves to a price.
# MAGIC
# MAGIC **References.** You may find these helpful:
# MAGIC - `3_4_scd2_approach.md` in this folder, the approach doc for this lab: both SCD Type 2 approaches side by side, with worked examples
# MAGIC - [Upsert into a Delta Lake table using merge](https://docs.databricks.com/aws/en/delta/merge)
# MAGIC - [Window functions](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-window-functions.html)
# MAGIC - [Tutorial: Create and manage Delta Lake tables](https://docs.databricks.com/aws/en/delta/tutorial)

# COMMAND ----------

# Your turn. Build silver_price_scd: distinct versions, shape the effective ranges with a window, then MERGE on price_sk.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Task 1: silver_price_scd: build the price history with a window and a MERGE (solution)
# MAGIC <details>
# MAGIC <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; overflow:auto;"></pre>
# MAGIC
# MAGIC <script>
# MAGIC var codeB64 = "YnJvbnplID0gZiJ7Y2F0YWxvZ30uaGVsaW9zX2Jyb256ZSIKc2lsdmVyID0gZiJ7Y2F0YWxvZ30uaGVsaW9zX3NpbHZlciIKCiMgU3RlcCAxOiBidWlsZCB0aGUgZnVsbCBwcmljZSBoaXN0b3J5IHdpdGggYSB3aW5kb3cuIFJlZHVjZSBCcm9uemUgdG8gdGhlIGRpc3RpbmN0IHZlcnNpb25zLCB0aGVuIGZvciBlYWNoCiMgcHJvZHVjdCBzZXQgZWZmZWN0aXZlX3RvIHRvIHRoZSBuZXh0IHZlcnNpb24ncyBzdGFydCAobGVhZCkgYW5kIG1hcmsgdGhlIG9wZW4gb25lIGN1cnJlbnQuIEdpdmUgZWFjaCBhIGtleS4KYnlfcHJvZHVjdCA9IFdpbmRvdy5wYXJ0aXRpb25CeSgicHJvZHVjdF9pZCIpLm9yZGVyQnkoImVmZmVjdGl2ZV9mcm9tIikKcHJpY2VfdmVyc2lvbnMgPSAoCiAgICBzcGFyay5yZWFkLnRhYmxlKGYie2Jyb256ZX0uYnJvbnplX3ByaWNlX2xpc3QiKQogICAgLnNlbGVjdCgicHJvZHVjdF9pZCIsICJzdXBwbGllcl9pZCIsCiAgICAgICAgICAgIGNvbCgidW5pdF9wcmljZSIpLmNhc3QoImRlY2ltYWwoMTIsMikiKS5hbGlhcygidW5pdF9wcmljZSIpLAogICAgICAgICAgICBjb2woInVuaXRfY29zdCIpLmNhc3QoImRlY2ltYWwoMTIsMikiKS5hbGlhcygidW5pdF9jb3N0IiksCiAgICAgICAgICAgICJjdXJyZW5jeSIsCiAgICAgICAgICAgIGNvbCgiZWZmZWN0aXZlX2Zyb20iKS5jYXN0KCJkYXRlIikuYWxpYXMoImVmZmVjdGl2ZV9mcm9tIikpCiAgICAuZHJvcER1cGxpY2F0ZXMoWyJwcm9kdWN0X2lkIiwgImVmZmVjdGl2ZV9mcm9tIl0pCiAgICAud2l0aENvbHVtbigiZWZmZWN0aXZlX3RvIiwgbGVhZCgiZWZmZWN0aXZlX2Zyb20iKS5vdmVyKGJ5X3Byb2R1Y3QpKQogICAgLndpdGhDb2x1bW4oImlzX2N1cnJlbnQiLCBjb2woImVmZmVjdGl2ZV90byIpLmlzTnVsbCgpKQogICAgLndpdGhDb2x1bW4oInByaWNlX3NrIiwgY29uY2F0X3dzKCJfIiwgY29sKCJwcm9kdWN0X2lkIiksIGRhdGVfZm9ybWF0KCJlZmZlY3RpdmVfZnJvbSIsICJ5eXl5TU1kZCIpKSkKKQoKIyBTdGVwIDI6IGNyZWF0ZSB0aGUgU0NEIFR5cGUgMiB0YWJsZSBpZiBpdCBkb2VzIG5vdCBleGlzdCB5ZXQuCnByaWNlX3ZlcnNpb25zLmxpbWl0KDApLndyaXRlLmZvcm1hdCgiZGVsdGEiKS5tb2RlKCJpZ25vcmUiKS5zYXZlQXNUYWJsZShmIntzaWx2ZXJ9LnNpbHZlcl9wcmljZV9zY2QiKQoKIyBTdGVwIDM6IE1FUkdFIHRoZSB2ZXJzaW9ucyBpbiBvbiBwcmljZV9zazogdXBkYXRlIHRoZSBvbmVzIGFscmVhZHkgdGhlcmUgKHNvIGEgbmV3bHkgY2xvc2VkIHJhbmdlIGlzCiMgcmVmcmVzaGVkKSwgaW5zZXJ0IHRoZSBuZXcgb25lcy4gT25lIG1lcmdlLCBpZGVtcG90ZW50LCB0b3VjaGluZyBvbmx5IHdoYXQgY2hhbmdlZC4KKERlbHRhVGFibGUuZm9yTmFtZShzcGFyaywgZiJ7c2lsdmVyfS5zaWx2ZXJfcHJpY2Vfc2NkIikuYWxpYXMoInRhcmdldCIpCiAgICAubWVyZ2UocHJpY2VfdmVyc2lvbnMuYWxpYXMoInNvdXJjZSIpLCAidGFyZ2V0LnByaWNlX3NrID0gc291cmNlLnByaWNlX3NrIikKICAgIC53aGVuTWF0Y2hlZFVwZGF0ZUFsbCgpCiAgICAud2hlbk5vdE1hdGNoZWRJbnNlcnRBbGwoKQogICAgLmV4ZWN1dGUoKSk=";
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

silver = f"{catalog}.helios_silver"
price = spark.table(f"{silver}.silver_price_scd")

check("price_sk is unique", price.count(), price.select("price_sk").distinct().count())
check("exactly one current row per product", 0,
      price.filter("is_current").groupBy("product_id").count().filter("count <> 1").count())
ranges = price.withColumn(
    "next_from", lead("effective_from").over(Window.partitionBy("product_id").orderBy("effective_from")))
check("effective ranges are contiguous and non overlapping", 0,
      ranges.filter("next_from IS NOT NULL AND (effective_to IS NULL OR effective_to <> next_from)").count())
check("PRD-001 has two versions after its price change", 2, price.filter("product_id = 'PRD-001'").count())
from pyspark.sql.functions import min as min_
starts = price.groupBy("product_id").agg(min_("effective_from").alias("first"))
check("every product is priced from the base date", 0, starts.filter("first <> DATE'2257-03-01'").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: silver_customer_scd: build the customer dimension as SCD Type 2
# MAGIC
# MAGIC Build `silver_customer_scd`, the customer dimension as a Slowly Changing Dimension Type 2, from the daily customer snapshots. This is the customer dimension itself (full attributes, not just tier), so there is no separate current customer table. Use the same window and MERGE, versioning a customer when its tier changes. `3_4_scd2_approach.md` sets both approaches out side by side, including why a snapshot feed needs a comparison across days.
# MAGIC
# MAGIC **How.**
# MAGIC - Read the customer snapshots from Bronze and keep one row per (`customer_id`, `snapshot_date`), typed.
# MAGIC - The snapshots carry no version dates, so find the changes with a window over each customer, ordered by `snapshot_date`: a new version starts where the tier differs from the previous snapshot (`lag`). Keep only those version-start rows.
# MAGIC - Set each version's `effective_to` to the next version's `snapshot_date` (`lead`), mark the open one current, give each a surrogate key `customer_sk`, and use the `snapshot_date` as `effective_from`.
# MAGIC - Create the `silver_customer_scd` table if it does not exist, then `MERGE` the versions in on `customer_sk`: update existing, insert new.
# MAGIC - The dimension carries the full customer attributes per version, so the Gold `dim_customer` reads straight from it.
# MAGIC
# MAGIC **Example.** `CUST-0033` was `DRIFTER` on the first snapshots, then `TRADE` from `2257-03-03`, so the dimension keeps two versions, carrying all of that customer's attributes in each:
# MAGIC
# MAGIC | customer_sk | tier | effective_from | effective_to | is_current |
# MAGIC |---|---|---|---|---|
# MAGIC | `CUST-0033_22570301` | `DRIFTER` | `2257-03-01` | `2257-03-03` | `false` |
# MAGIC | `CUST-0033_22570303` | `TRADE` | `2257-03-03` | `null` | `true` |
# MAGIC
# MAGIC **Definition of done.**
# MAGIC - `silver_customer_scd` carries the full customer attributes plus `customer_sk`, `effective_from`, `effective_to`, `is_current`.
# MAGIC - Exactly one current row per customer, and `customer_sk` is unique.
# MAGIC - A customer whose tier changed has more than one version, with contiguous, non overlapping ranges; `CUST-0033` has two.
# MAGIC
# MAGIC **References.** You may find these helpful:
# MAGIC - `3_4_scd2_approach.md` in this folder, the approach doc for this lab: both SCD Type 2 approaches side by side, with worked examples
# MAGIC - [Upsert into a Delta Lake table using merge](https://docs.databricks.com/aws/en/delta/merge)
# MAGIC - [Window functions](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-window-functions.html)
# MAGIC - [Data modeling](https://docs.databricks.com/aws/en/transform/data-modeling)

# COMMAND ----------

# Your turn. Build silver_customer_scd: the full customer SCD2, version on tier change with a window, then MERGE on customer_sk.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Task 2: silver_customer_scd: build the customer dimension as SCD Type 2 (solution)
# MAGIC <details>
# MAGIC <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; overflow:auto;"></pre>
# MAGIC
# MAGIC <script>
# MAGIC var codeB64 = "YnJvbnplID0gZiJ7Y2F0YWxvZ30uaGVsaW9zX2Jyb256ZSIKc2lsdmVyID0gZiJ7Y2F0YWxvZ30uaGVsaW9zX3NpbHZlciIKCiMgU3RlcCAxOiBidWlsZCB0aGUgdGllciBoaXN0b3J5IHdpdGggYSB3aW5kb3cuIEEgbmV3IHZlcnNpb24gc3RhcnRzIHdoZW4gYSBjdXN0b21lcidzIHRpZXIgZGlmZmVycyBmcm9tIHRoZQojIHByZXZpb3VzIHNuYXBzaG90IChsYWcpOyBlZmZlY3RpdmVfdG8gaXMgdGhlIG5leHQgdmVyc2lvbidzIHN0YXJ0IChsZWFkKTsgdGhlIG9wZW4gb25lIGlzIGN1cnJlbnQuCmJ5X2N1c3RvbWVyID0gV2luZG93LnBhcnRpdGlvbkJ5KCJjdXN0b21lcl9pZCIpLm9yZGVyQnkoInNuYXBzaG90X2RhdGUiKQpjdXN0b21lcl92ZXJzaW9ucyA9ICgKICAgIHNwYXJrLnJlYWQudGFibGUoZiJ7YnJvbnplfS5icm9uemVfY3VzdG9tZXJzIikKICAgIC5zZWxlY3QoImN1c3RvbWVyX2lkIiwgImN1c3RvbWVyX25hbWUiLCAiY3VzdG9tZXJfdHlwZSIsICJ0aWVyIiwgImhvbWVfd2FyZWhvdXNlX2lkIiwKICAgICAgICAgICAgY29sKCJzaWdudXBfZGF0ZSIpLmNhc3QoImRhdGUiKS5hbGlhcygic2lnbnVwX2RhdGUiKSwKICAgICAgICAgICAgY29sKCJzbmFwc2hvdF9kYXRlIikuY2FzdCgiZGF0ZSIpLmFsaWFzKCJzbmFwc2hvdF9kYXRlIikpCiAgICAuZHJvcER1cGxpY2F0ZXMoWyJjdXN0b21lcl9pZCIsICJzbmFwc2hvdF9kYXRlIl0pCiAgICAud2l0aENvbHVtbigicHJldl90aWVyIiwgbGFnKCJ0aWVyIikub3ZlcihieV9jdXN0b21lcikpCiAgICAuZmlsdGVyKGNvbCgicHJldl90aWVyIikuaXNOdWxsKCkgfCAoY29sKCJ0aWVyIikgIT0gY29sKCJwcmV2X3RpZXIiKSkpICAgIyBrZWVwIG9ubHkgdGhlIHZlcnNpb24gc3RhcnRzCiAgICAud2l0aENvbHVtbigiZWZmZWN0aXZlX3RvIiwgbGVhZCgic25hcHNob3RfZGF0ZSIpLm92ZXIoYnlfY3VzdG9tZXIpKQogICAgLndpdGhDb2x1bW4oImlzX2N1cnJlbnQiLCBjb2woImVmZmVjdGl2ZV90byIpLmlzTnVsbCgpKQogICAgLndpdGhDb2x1bW4oImN1c3RvbWVyX3NrIiwgY29uY2F0X3dzKCJfIiwgY29sKCJjdXN0b21lcl9pZCIpLCBkYXRlX2Zvcm1hdCgic25hcHNob3RfZGF0ZSIsICJ5eXl5TU1kZCIpKSkKICAgIC53aXRoQ29sdW1uUmVuYW1lZCgic25hcHNob3RfZGF0ZSIsICJlZmZlY3RpdmVfZnJvbSIpCiAgICAuc2VsZWN0KCJjdXN0b21lcl9zayIsICJjdXN0b21lcl9pZCIsICJjdXN0b21lcl9uYW1lIiwgImN1c3RvbWVyX3R5cGUiLCAidGllciIsICJob21lX3dhcmVob3VzZV9pZCIsCiAgICAgICAgICAgICJzaWdudXBfZGF0ZSIsICJlZmZlY3RpdmVfZnJvbSIsICJlZmZlY3RpdmVfdG8iLCAiaXNfY3VycmVudCIpCikKCiMgU3RlcCAyOiBjcmVhdGUgdGhlIFNDRCBUeXBlIDIgdGFibGUgaWYgaXQgZG9lcyBub3QgZXhpc3QgeWV0LgpjdXN0b21lcl92ZXJzaW9ucy5saW1pdCgwKS53cml0ZS5mb3JtYXQoImRlbHRhIikubW9kZSgiaWdub3JlIikuc2F2ZUFzVGFibGUoZiJ7c2lsdmVyfS5zaWx2ZXJfY3VzdG9tZXJfc2NkIikKCiMgU3RlcCAzOiBNRVJHRSB0aGUgdmVyc2lvbnMgaW4gb24gY3VzdG9tZXJfc2s6IHVwZGF0ZSBleGlzdGluZywgaW5zZXJ0IG5ldy4gT25lIG1lcmdlLCBpZGVtcG90ZW50LgooRGVsdGFUYWJsZS5mb3JOYW1lKHNwYXJrLCBmIntzaWx2ZXJ9LnNpbHZlcl9jdXN0b21lcl9zY2QiKS5hbGlhcygidGFyZ2V0IikKICAgIC5tZXJnZShjdXN0b21lcl92ZXJzaW9ucy5hbGlhcygic291cmNlIiksICJ0YXJnZXQuY3VzdG9tZXJfc2sgPSBzb3VyY2UuY3VzdG9tZXJfc2siKQogICAgLndoZW5NYXRjaGVkVXBkYXRlQWxsKCkKICAgIC53aGVuTm90TWF0Y2hlZEluc2VydEFsbCgpCiAgICAuZXhlY3V0ZSgpKQ==";
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
customer = spark.table(f"{silver}.silver_customer_scd")

check("customer_sk is unique", customer.count(), customer.select("customer_sk").distinct().count())
check("exactly one current row per customer", 0,
      customer.filter("is_current").groupBy("customer_id").count().filter("count <> 1").count())
check("the dimension carries the full customer attributes", True,
      {"customer_name", "customer_type", "home_warehouse_id", "signup_date"}.issubset(set(customer.columns)))
check("CUST-0033 has two versions (DRIFTER then TRADE)", 2,
      customer.filter("customer_id = 'CUST-0033'").count())
ranges = customer.withColumn(
    "next_from", lead("effective_from").over(Window.partitionBy("customer_id").orderBy("effective_from")))
check("effective ranges are contiguous and non overlapping", 0,
      ranges.filter("next_from IS NOT NULL AND (effective_to IS NULL OR effective_to <> next_from)").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## What you built
# MAGIC
# MAGIC Two SCD Type 2 dimensions, both built the same way: a window shapes the version history (the effective ranges and the current flag), then a single Delta MERGE applies it (update existing, insert new). `silver_price_scd` comes from the effective dated `price_list`, and `silver_customer_scd`, the customer dimension itself, from the daily snapshots. Each keeps one current row per key and a clean, contiguous effective history, so the Gold fact can value every order line at the price in force, and `dim_customer` carries each customer's tier as it changes over time.