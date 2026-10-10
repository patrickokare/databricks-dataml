# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 3.8: Silver: type, dedupe, quality and CDC
# MAGIC
# MAGIC ### Ticket: HELIOS-308
# MAGIC
# MAGIC **Context.** Bronze is raw and messy by design. Silver is where it becomes trustworthy: every column correctly typed, one row per business key, bad rows quarantined rather than dropped, the order change feed reduced to current state, and returns checked against the lines they point at. You build it with the DataFrame and Delta Lake Python APIs, matching the write to the shape of each source: upsert the append and CDC feeds, overwrite the full snapshots.
# MAGIC
# MAGIC **Tasks.**
# MAGIC 1. Write a reusable `upsert` helper, used by the tables that follow.
# MAGIC 2. `silver_order_lines`: type, dedupe and quarantine the order line stream.
# MAGIC 3. `silver_orders`: reduce the order CDC feed to current state.
# MAGIC 4. `silver_returns`: type returns and quarantine the ones that break the rules.
# MAGIC 5. `silver` reference dimensions and inventory: overwrite the four reference dimensions and upsert the inventory movement log.
# MAGIC
# MAGIC **Acceptance criteria** (the self checks): 
# MAGIC - every Silver table is explicitly typed and has one row per business key; 
# MAGIC - `silver_order_lines` carries no duplicate `order_line_id` and every clean row has a positive quantity and a product, with the violations in `silver_order_lines_quarantine`; 
# MAGIC - `silver_orders` holds one current row per `order_id` with cancelled and purged orders removed; 
# MAGIC - `silver_returns` points only at real lines and is dated after them, with orphans quarantined; 
# MAGIC - the reference dimensions and the inventory log have one row per key. (Customers become an SCD Type 2 dimension in Lab 3.9, so there is no plain customer table.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bootstrap
# MAGIC Starts from a clean slate so the lab is deterministic on every run: reset the Bronze and Silver schemas (which also clears the streaming checkpoints), clear the landing Volume, and re-land batches one and two. Batch two is when the order lifecycle starts moving (CDC updates), the first returns arrive, and late and duplicate records appear, so Silver has real work to do. Then it rebuilds Bronze by running the production Bronze task notebook.

# COMMAND ----------

# MAGIC %run ../00_setup/data_generator

# COMMAND ----------

# MAGIC %run ../00_setup/bootstrap_helpers

# COMMAND ----------

from pyspark.sql.functions import col, row_number
from pyspark.sql import Window
from delta.tables import DeltaTable

# Clean slate: reset Bronze (rebuilt below) and Silver (which you build), and re-land batches one and two, so
# the lab is deterministic on every run. Resetting a schema also clears its checkpoints Volume.
catalog = helios_identity()
for layer in ["helios_bronze", "helios_silver"]:
    reset_schema(catalog, layer)
clear_landing(catalog)
ensure_landing(catalog, up_to_batch=2)

# COMMAND ----------

# MAGIC %run ./job_tasks/01_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Write a reusable upsert helper
# MAGIC
# MAGIC Write a reusable `upsert(source, table, keys)` helper that you will use for most of the Silver and Gold tables. It makes sure the target table exists, then merges new rows into it on a key, updating the rows that match and inserting the rest. `3_3_silver_approach.md`, in this folder, covers why nearly every table in this layer is written this way.
# MAGIC
# MAGIC **How.**
# MAGIC - Almost every load does the same thing: make sure the target exists, then merge new rows into it on a key. Writing it once keeps the table-building code short and keeps the merge logic in one place.
# MAGIC - Create the target table from the source's schema if it does not already exist, then merge on the key column(s): update the matched rows, insert the unmatched ones.
# MAGIC - Accept `keys` as either a single column name or a list, so the same helper works for single and composite keys (the inventory log keys on three columns).
# MAGIC - The CDC feed is a special case you still write out in full, and the full snapshots are overwritten, see the later tasks.
# MAGIC
# MAGIC **Definition of done.**
# MAGIC - `upsert(source, table, keys)` is defined and reusable.
# MAGIC - If the target table does not exist, it is created from the source's schema.
# MAGIC - The merge updates rows whose key matches and inserts rows whose key is new.
# MAGIC - `keys` accepts either a single column name or a list of column names.
# MAGIC
# MAGIC **References.** You may find these helpful:
# MAGIC - `3_3_silver_approach.md` in this folder, the approach doc for this lab: the write pattern for each feed shape, the quality rules and worked examples
# MAGIC - [Upsert into a Delta Lake table using merge](https://docs.databricks.com/aws/en/delta/merge)
# MAGIC - [Tutorial: Create and manage Delta Lake tables](https://docs.databricks.com/aws/en/delta/tutorial)

# COMMAND ----------

# Your turn. Write upsert(source, table, keys).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Task 1: Write a reusable upsert helper (solution)
# MAGIC <details>
# MAGIC <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; overflow:auto;"></pre>
# MAGIC
# MAGIC <script>
# MAGIC var codeB64 = "ZGVmIHVwc2VydChzb3VyY2UsIHRhYmxlLCBrZXlzKToKICAgICIiIkNyZWF0ZSB0aGUgdGFyZ2V0IGlmIG5lZWRlZCwgdGhlbiBNRVJHRSB0aGUgc291cmNlIGludG8gaXQgb24gdGhlIGtleShzKS4gSWRlbXBvdGVudCBhbmQgaW5jcmVtZW50YWwuIiIiCiAgICBrZXlzID0gW2tleXNdIGlmIGlzaW5zdGFuY2Uoa2V5cywgc3RyKSBlbHNlIGtleXMKICAgIHNvdXJjZS5saW1pdCgwKS53cml0ZS5mb3JtYXQoImRlbHRhIikubW9kZSgiaWdub3JlIikuc2F2ZUFzVGFibGUodGFibGUpCiAgICBjb25kaXRpb24gPSAiIEFORCAiLmpvaW4oZiJ0LntrfSA9IHMue2t9IiBmb3IgayBpbiBrZXlzKQogICAgKERlbHRhVGFibGUuZm9yTmFtZShzcGFyaywgdGFibGUpLmFsaWFzKCJ0IikKICAgICAgICAubWVyZ2Uoc291cmNlLmFsaWFzKCJzIiksIGNvbmRpdGlvbikKICAgICAgICAud2hlbk1hdGNoZWRVcGRhdGVBbGwoKS53aGVuTm90TWF0Y2hlZEluc2VydEFsbCgpLmV4ZWN1dGUoKSk=";
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
# MAGIC Exercises `upsert` on a throwaway table: insert two rows, then upsert an update to one key plus a brand new key, and confirm the merge updated in place, inserted the new key, and left the untouched key alone.

# COMMAND ----------

test_table = f"{catalog}.helios_silver._upsert_selftest"
spark.sql(f"DROP TABLE IF EXISTS {test_table}")

upsert(spark.createDataFrame([("A", 1), ("B", 2)], "id string, val int"), test_table, "id")
check("upsert inserts new keys", 2, spark.table(test_table).count())

upsert(spark.createDataFrame([("B", 99), ("C", 3)], "id string, val int"), test_table, "id")  # update B, add C
state = {r["id"]: r["val"] for r in spark.table(test_table).collect()}
check("upsert updates a matched key in place", 99, state.get("B"))
check("upsert leaves an untouched key alone", 1, state.get("A"))
check("upsert inserts a brand new key", 3, state.get("C"))
check("no duplicate keys after upsert", 3, spark.table(test_table).count())

spark.sql(f"DROP TABLE IF EXISTS {test_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: silver_order_lines: type, dedupe and quarantine the order line stream
# MAGIC
# MAGIC Build a clean, typed `silver_order_lines` with one row per `order_line_id`, streaming it from Bronze and moving the rows that break the data contract into a `silver_order_lines_quarantine` table instead of dropping them. The dedupe and the two upserts are batch operations, so you run them on each micro-batch inside a `foreachBatch` function. The dedupe rule, the quarantine rules and the streaming pattern are set out in `3_3_silver_approach.md`.
# MAGIC
# MAGIC **How.**
# MAGIC - `order_lines` arrives as raw JSON: every column is a string, the source delivers each line at least once (so the same `order_line_id` repeats), and a few rows break the contract with a zero or negative quantity or a missing product.
# MAGIC - Read Bronze as a stream with a checkpoint, so a re-run only processes new files.
# MAGIC - The stream needs somewhere to keep that checkpoint, so create a checkpoints Volume and point it at a generic path, `/Volumes/<catalog>/helios_silver/checkpoints/silver_order_lines`.
# MAGIC - Type every column to the target schema below.
# MAGIC - Drive the streaming write with `foreachBatch`: the dedupe and the two upserts are batch operations (a window and a Delta MERGE) that a streaming query cannot run directly, so they go in a function Spark calls once per micro-batch. The next three steps all happen inside that function.
# MAGIC - Keep one row per `order_line_id`: the latest by `line_ts`.
# MAGIC - Split the rows: the ones that obey the rules go to `silver_order_lines`, the ones that break them (`quantity` null or not positive, or `product_id` null) go to `silver_order_lines_quarantine`. Quarantining keeps a data quality problem visible to investigate, rather than letting it vanish or poison Gold.
# MAGIC - Write each table with your `upsert` helper keyed on `order_line_id`, so reprocessing a line updates its row rather than duplicating it.
# MAGIC
# MAGIC **Target schema** (`silver_order_lines`; the quarantine table is identical):
# MAGIC
# MAGIC | column | type |
# MAGIC |---|---|
# MAGIC | order_line_id | string |
# MAGIC | order_id | string |
# MAGIC | product_id | string |
# MAGIC | customer_id | string |
# MAGIC | warehouse_id | string |
# MAGIC | quantity | int |
# MAGIC | line_ts | timestamp |
# MAGIC
# MAGIC **Example.** A clean row, the first line of batch one:
# MAGIC
# MAGIC | order_line_id | order_id | product_id | warehouse_id | quantity | line_ts |
# MAGIC |---|---|---|---|---|---|
# MAGIC | `OL-1-000001` | `ORD-000001` | `PRD-017` | `DEP-01` | `2` | `2257-03-01 13:47:04` |
# MAGIC
# MAGIC **Definition of done.**
# MAGIC - `silver_order_lines` has one row per `order_line_id` (no duplicates).
# MAGIC - Every column is typed as above (`quantity` is `int`, `line_ts` is `timestamp`).
# MAGIC - Every clean row has `quantity` greater than zero and a non-null `product_id`.
# MAGIC - The contract-breaking rows are in `silver_order_lines_quarantine`, not in the clean table.
# MAGIC
# MAGIC **References.** You may find these helpful:
# MAGIC - `3_3_silver_approach.md` in this folder, the approach doc for this lab: the write pattern for each feed shape, the quality rules and worked examples
# MAGIC - [Delta Lake table streaming reads and writes](https://docs.databricks.com/aws/en/structured-streaming/delta-lake)
# MAGIC - [Upsert into a Delta Lake table using merge](https://docs.databricks.com/aws/en/delta/merge)
# MAGIC - [Structured Streaming checkpoints](https://docs.databricks.com/aws/en/structured-streaming/checkpoints)
# MAGIC - [Use foreachBatch to write to arbitrary data sinks](https://docs.databricks.com/aws/en/structured-streaming/foreach)

# COMMAND ----------

# Your turn. Stream order_lines into silver_order_lines (clean) and the quarantine table.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Task 2: silver_order_lines: type, dedupe and quarantine the order line stream (solution)
# MAGIC <details>
# MAGIC <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; overflow:auto;"></pre>
# MAGIC
# MAGIC <script>
# MAGIC var codeB64 = "YnJvbnplID0gZiJ7Y2F0YWxvZ30uaGVsaW9zX2Jyb256ZSIKc2lsdmVyID0gZiJ7Y2F0YWxvZ30uaGVsaW9zX3NpbHZlciIKCiMgQSBjaGVja3BvaW50IHZvbHVtZSBsZXRzIHRoZSBzdHJlYW0gcmVzdW1lIGFuZCBwcm9jZXNzIG9ubHkgbmV3IGZpbGVzIG9uIGEgcmUtcnVuLgpzcGFyay5zcWwoZiJDUkVBVEUgVk9MVU1FIElGIE5PVCBFWElTVFMge2NhdGFsb2d9LmhlbGlvc19zaWx2ZXIuY2hlY2twb2ludHMiKQpjaGVja3BvaW50cyA9IGYiL1ZvbHVtZXMve2NhdGFsb2d9L2hlbGlvc19zaWx2ZXIvY2hlY2twb2ludHMiCgoKZGVmIHRvX3NpbHZlcl9vcmRlcl9saW5lcyhtaWNyb19iYXRjaCwgX2JhdGNoX2lkKToKICAgICMgS2VlcCBvbmUgcm93IHBlciBvcmRlcl9saW5lX2lkOiB0aGUgbW9zdCByZWNlbnQgYnkgbGluZV90cy4KICAgIGxhdGVzdF9wZXJfbGluZSA9IFdpbmRvdy5wYXJ0aXRpb25CeSgib3JkZXJfbGluZV9pZCIpLm9yZGVyQnkoY29sKCJsaW5lX3RzIikuZGVzY19udWxsc19sYXN0KCkpCiAgICBkZWR1cGVkID0gKAogICAgICAgIG1pY3JvX2JhdGNoCiAgICAgICAgLndpdGhDb2x1bW4oIl9ybiIsIHJvd19udW1iZXIoKS5vdmVyKGxhdGVzdF9wZXJfbGluZSkpCiAgICAgICAgLmZpbHRlcihjb2woIl9ybiIpID09IDEpCiAgICAgICAgLmRyb3AoIl9ybiIpCiAgICApCgogICAgIyBBIHJvdyBicmVha3MgdGhlIGNvbnRyYWN0IGlmIHRoZSBxdWFudGl0eSBpcyBtaXNzaW5nIG9yIG5vdCBwb3NpdGl2ZSwgb3IgdGhlIHByb2R1Y3QgaXMgbWlzc2luZy4KICAgIGJyZWFrc19jb250cmFjdCA9IGNvbCgicXVhbnRpdHkiKS5pc051bGwoKSB8IChjb2woInF1YW50aXR5IikgPD0gMCkgfCBjb2woInByb2R1Y3RfaWQiKS5pc051bGwoKQogICAgY2xlYW4gPSBkZWR1cGVkLmZpbHRlcih+YnJlYWtzX2NvbnRyYWN0KQogICAgcXVhcmFudGluZSA9IGRlZHVwZWQuZmlsdGVyKGJyZWFrc19jb250cmFjdCkKCiAgICB1cHNlcnQoY2xlYW4sIGYie3NpbHZlcn0uc2lsdmVyX29yZGVyX2xpbmVzIiwgIm9yZGVyX2xpbmVfaWQiKQogICAgdXBzZXJ0KHF1YXJhbnRpbmUsIGYie3NpbHZlcn0uc2lsdmVyX29yZGVyX2xpbmVzX3F1YXJhbnRpbmUiLCAib3JkZXJfbGluZV9pZCIpCgoKIyBUeXBlIGV2ZXJ5IGNvbHVtbiBleHBsaWNpdGx5IGFzIHdlIHJlYWQgdGhlIHJhdyBzdHJlYW0gKEpTT04gZGVsaXZlcmVkIHRoZW0gYWxsIGFzIHN0cmluZ3MpLgp0eXBlZF9zdHJlYW0gPSAoCiAgICBzcGFyay5yZWFkU3RyZWFtLnRhYmxlKGYie2Jyb256ZX0uYnJvbnplX29yZGVyX2xpbmVzIikKICAgIC5zZWxlY3QoCiAgICAgICAgY29sKCJvcmRlcl9saW5lX2lkIikuY2FzdCgic3RyaW5nIikuYWxpYXMoIm9yZGVyX2xpbmVfaWQiKSwKICAgICAgICBjb2woIm9yZGVyX2lkIikuY2FzdCgic3RyaW5nIikuYWxpYXMoIm9yZGVyX2lkIiksCiAgICAgICAgY29sKCJwcm9kdWN0X2lkIikuY2FzdCgic3RyaW5nIikuYWxpYXMoInByb2R1Y3RfaWQiKSwKICAgICAgICBjb2woImN1c3RvbWVyX2lkIikuY2FzdCgic3RyaW5nIikuYWxpYXMoImN1c3RvbWVyX2lkIiksCiAgICAgICAgY29sKCJ3YXJlaG91c2VfaWQiKS5jYXN0KCJzdHJpbmciKS5hbGlhcygid2FyZWhvdXNlX2lkIiksCiAgICAgICAgY29sKCJxdWFudGl0eSIpLmNhc3QoImludCIpLmFsaWFzKCJxdWFudGl0eSIpLAogICAgICAgIGNvbCgibGluZV90cyIpLmNhc3QoInRpbWVzdGFtcCIpLmFsaWFzKCJsaW5lX3RzIiksCiAgICApCikKCigKICAgIHR5cGVkX3N0cmVhbS53cml0ZVN0cmVhbQogICAgLmZvcmVhY2hCYXRjaCh0b19zaWx2ZXJfb3JkZXJfbGluZXMpCiAgICAub3B0aW9uKCJjaGVja3BvaW50TG9jYXRpb24iLCBmIntjaGVja3BvaW50c30vc2lsdmVyX29yZGVyX2xpbmVzIikKICAgIC50cmlnZ2VyKGF2YWlsYWJsZU5vdz1UcnVlKSAgICAgICAgICAjIHByb2Nlc3Mgd2hhdCBoYXMgbGFuZGVkLCB0aGVuIHN0b3AgKHRoaXMgcnVucyBhcyBhIGJhdGNoIEpvYikKICAgIC5zdGFydCgpCiAgICAuYXdhaXRUZXJtaW5hdGlvbigpICAgICAgICAgICAgICAgICAgIyBibG9jayB0aGlzIGNlbGwgdW50aWwgaW5nZXN0aW9uIGhhcyBmaW5pc2hlZAop";
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
lines = spark.table(f"{silver}.silver_order_lines")
quarantine = spark.table(f"{silver}.silver_order_lines_quarantine")

rule = "quantity IS NULL OR quantity <= 0 OR product_id IS NULL"

check("one row per order_line_id (no duplicates)", lines.count(), lines.select("order_line_id").distinct().count())
check("quantity is typed as an int", "int", dict(lines.dtypes)["quantity"])
check("line_ts is typed as a timestamp", "timestamp", dict(lines.dtypes)["line_ts"])
check("no clean row breaks the quantity or product rule", 0, lines.filter(rule).count())
check("every quarantined row really breaks a rule", 0, quarantine.filter(f"NOT ({rule})").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: silver_orders: reduce the CDC feed to current state
# MAGIC
# MAGIC Reduce the order change feed down to current state: build `silver_orders` with one row per `order_id` holding its latest values, and cancelled and purged orders removed. `3_3_silver_approach.md` covers the change feed, including why recency comes from `change_seq` rather than arrival order.
# MAGIC
# MAGIC **How.**
# MAGIC - Orders arrive as a change feed, change data capture, or CDC. Each row in `bronze_orders` is one change to an order (placed, paid, picked, shipped, delivered, cancelled), so a single order is spread across several rows and several batches. This is how many operational systems share data without re-sending everything each time.
# MAGIC - For each `order_id`, find its most recent change. Changes can arrive out of order, so recency is decided by `change_seq` (higher is later), not by arrival order.
# MAGIC - Apply that latest change: if it is a `DELETE` the order was cancelled and purged at the source, so remove it from Silver; otherwise insert or update the order to its new values.
# MAGIC - Type the columns to the target schema below, and do not carry the CDC bookkeeping column `op` into Silver.
# MAGIC
# MAGIC **Target schema** (`silver_orders`):
# MAGIC
# MAGIC | column | type |
# MAGIC |---|---|
# MAGIC | order_id | string |
# MAGIC | customer_id | string |
# MAGIC | warehouse_id | string |
# MAGIC | channel | string |
# MAGIC | order_ts | timestamp |
# MAGIC | status | string |
# MAGIC | change_seq | bigint |
# MAGIC | change_ts | timestamp |
# MAGIC
# MAGIC **Example.** `ORD-000001` is placed on batch one and progresses over batch two, and the row with the highest `change_seq` wins, so Silver keeps it at `DELIVERED`:
# MAGIC
# MAGIC | order_id | status | change_seq |
# MAGIC |---|---|---|
# MAGIC | `ORD-000001` | `PLACED` | `1` |
# MAGIC | `ORD-000001` | `PAID` | `2` |
# MAGIC | `ORD-000001` | `PICKED` | `3` |
# MAGIC | `ORD-000001` | `SHIPPED` | `4` |
# MAGIC | `ORD-000001` | `DELIVERED` | `5`  (kept) |
# MAGIC
# MAGIC **Definition of done.**
# MAGIC - `silver_orders` has one row per `order_id` (no duplicates).
# MAGIC - Each row reflects the change with the highest `change_seq` for that order.
# MAGIC - An order whose latest change is `op = 'DELETE'` is absent from the table.
# MAGIC - The columns are typed as above, and there is no `op` column.
# MAGIC
# MAGIC **References.** You may find these helpful:
# MAGIC - `3_3_silver_approach.md` in this folder, the approach doc for this lab: the write pattern for each feed shape, the quality rules and worked examples
# MAGIC - [Upsert into a Delta Lake table using merge](https://docs.databricks.com/aws/en/delta/merge)
# MAGIC - [Window functions](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-window-functions.html)
# MAGIC - [Delta Lake table streaming reads and writes](https://docs.databricks.com/aws/en/structured-streaming/delta-lake)

# COMMAND ----------

# Your turn. Build silver_orders: one current row per order_id, purged orders removed.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Task 3: silver_orders: reduce the CDC feed to current state (solution)
# MAGIC <details>
# MAGIC <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; overflow:auto;"></pre>
# MAGIC
# MAGIC <script>
# MAGIC var codeB64 = "YnJvbnplID0gZiJ7Y2F0YWxvZ30uaGVsaW9zX2Jyb256ZSIKc2lsdmVyID0gZiJ7Y2F0YWxvZ30uaGVsaW9zX3NpbHZlciIKCiMgRWFjaCByb3cgaW4gYnJvbnplX29yZGVycyBpcyBvbmUgQ0RDIGNoYW5nZS4gUmVkdWNlIHRvIHRoZSBsYXRlc3QgY2hhbmdlIHBlciBvcmRlciAoYnkgY2hhbmdlX3NlcSwgYmVjYXVzZQojIGNoYW5nZXMgY2FuIGFycml2ZSBvdXQgb2Ygb3JkZXIpLCB0eXBpbmcgZXZlcnkgY29sdW1uIG9uIHRoZSB3YXkuCmxhdGVzdF9wZXJfb3JkZXIgPSBXaW5kb3cucGFydGl0aW9uQnkoIm9yZGVyX2lkIikub3JkZXJCeShjb2woImNoYW5nZV9zZXEiKS5kZXNjKCkpCmxhdGVzdF9jaGFuZ2UgPSAoCiAgICBzcGFyay5yZWFkLnRhYmxlKGYie2Jyb256ZX0uYnJvbnplX29yZGVycyIpCiAgICAuc2VsZWN0KAogICAgICAgIGNvbCgib3JkZXJfaWQiKS5jYXN0KCJzdHJpbmciKS5hbGlhcygib3JkZXJfaWQiKSwKICAgICAgICBjb2woImN1c3RvbWVyX2lkIikuY2FzdCgic3RyaW5nIikuYWxpYXMoImN1c3RvbWVyX2lkIiksCiAgICAgICAgY29sKCJ3YXJlaG91c2VfaWQiKS5jYXN0KCJzdHJpbmciKS5hbGlhcygid2FyZWhvdXNlX2lkIiksCiAgICAgICAgY29sKCJjaGFubmVsIikuY2FzdCgic3RyaW5nIikuYWxpYXMoImNoYW5uZWwiKSwKICAgICAgICBjb2woIm9yZGVyX3RzIikuY2FzdCgidGltZXN0YW1wIikuYWxpYXMoIm9yZGVyX3RzIiksCiAgICAgICAgY29sKCJzdGF0dXMiKS5jYXN0KCJzdHJpbmciKS5hbGlhcygic3RhdHVzIiksCiAgICAgICAgY29sKCJvcCIpLmNhc3QoInN0cmluZyIpLmFsaWFzKCJvcCIpLAogICAgICAgIGNvbCgiY2hhbmdlX3NlcSIpLmNhc3QoImxvbmciKS5hbGlhcygiY2hhbmdlX3NlcSIpLAogICAgICAgIGNvbCgiY2hhbmdlX3RzIikuY2FzdCgidGltZXN0YW1wIikuYWxpYXMoImNoYW5nZV90cyIpLAogICAgKQogICAgLndpdGhDb2x1bW4oIl9ybiIsIHJvd19udW1iZXIoKS5vdmVyKGxhdGVzdF9wZXJfb3JkZXIpKQogICAgLmZpbHRlcihjb2woIl9ybiIpID09IDEpCiAgICAuZHJvcCgiX3JuIikKKQoKIyBTaWx2ZXIgaG9sZHMgZXZlcnl0aGluZyBleGNlcHQgdGhlIENEQyBvcCBmbGFnLgpzaWx2ZXJfY29scyA9IFsib3JkZXJfaWQiLCAiY3VzdG9tZXJfaWQiLCAid2FyZWhvdXNlX2lkIiwgImNoYW5uZWwiLCAib3JkZXJfdHMiLAogICAgICAgICAgICAgICAic3RhdHVzIiwgImNoYW5nZV9zZXEiLCAiY2hhbmdlX3RzIl0KCiMgQSBsYXRlc3QgY2hhbmdlIG9mIERFTEVURSBtZWFucyB0aGUgb3JkZXIgd2FzIGNhbmNlbGxlZCBhbmQgcHVyZ2VkOyBldmVyeXRoaW5nIGVsc2UgaXMgYSBsaXZlIG9yZGVyLgp0b19yZW1vdmUgPSBsYXRlc3RfY2hhbmdlLmZpbHRlcihjb2woIm9wIikgPT0gIkRFTEVURSIpLnNlbGVjdCgib3JkZXJfaWQiKQp0b191cHNlcnQgPSBsYXRlc3RfY2hhbmdlLmZpbHRlcihjb2woIm9wIikgIT0gIkRFTEVURSIpLnNlbGVjdCgqc2lsdmVyX2NvbHMpCgojIEluc2VydCBvciB1cGRhdGUgdGhlIGxpdmUgb3JkZXJzIHdpdGggdGhlIGhlbHBlciBmcm9tIGVhcmxpZXIuLi4KdXBzZXJ0KHRvX3Vwc2VydCwgZiJ7c2lsdmVyfS5zaWx2ZXJfb3JkZXJzIiwgIm9yZGVyX2lkIikKCiMgLi4udGhlbiByZW1vdmUgYW55IHB1cmdlZCBvcmRlcnMgdGhhdCBhcmUgYWxyZWFkeSBpbiBTaWx2ZXIuCigKICAgIERlbHRhVGFibGUuZm9yTmFtZShzcGFyaywgZiJ7c2lsdmVyfS5zaWx2ZXJfb3JkZXJzIikuYWxpYXMoInQiKQogICAgLm1lcmdlKHRvX3JlbW92ZS5hbGlhcygicyIpLCAidC5vcmRlcl9pZCA9IHMub3JkZXJfaWQiKQogICAgLndoZW5NYXRjaGVkRGVsZXRlKCkKICAgIC5leGVjdXRlKCkKKQ==";
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
bronze = f"{catalog}.helios_bronze"
orders = spark.table(f"{silver}.silver_orders")

check("one row per order_id", orders.count(), orders.select("order_id").distinct().count())
check_true("the CDC op flag was not carried into Silver", "op" not in orders.columns)
check("change_seq is typed as bigint", "bigint", dict(orders.dtypes)["change_seq"])

# Prove "latest change wins" without pinning a status: Silver's change_seq for a known order equals the
# maximum change_seq for that order in Bronze (cast, since Bronze is still string typed).
from pyspark.sql.functions import max as max_
oid = "ORD-000001"
silver_seq = orders.filter(f"order_id = '{oid}'").first()["change_seq"]
bronze_seq = (spark.table(f"{bronze}.bronze_orders")
              .filter(f"order_id = '{oid}'")
              .agg(max_(col("change_seq").cast("long"))).first()[0])
check(f"{oid} reflects its latest change_seq", bronze_seq, silver_seq)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: silver_returns: type returns and quarantine the rules breakers
# MAGIC
# MAGIC Build a typed `silver_returns` that holds only returns pointing at a real order line and dated after it, moving the rest (orphans that point at no line, or a return dated on or before its line) into `silver_returns_quarantine`. The quarantine rules for both feeds are in `3_3_silver_approach.md`.
# MAGIC
# MAGIC **How.**
# MAGIC - Returns arrive from batch two (nothing is delivered on day one), as an append feed. Each return points back to an `order_line_id` and carries a `return_ts`.
# MAGIC - Type the columns to the target schema below.
# MAGIC - A return is valid only if its `order_line_id` exists in `silver_order_lines` and its `return_ts` is strictly later than that line's `line_ts`. Join to `silver_order_lines` to check both.
# MAGIC - Send the valid returns to `silver_returns` and the rest to `silver_returns_quarantine` (the orphan returns are a deliberate data quality case), both upserted on `return_id`.
# MAGIC
# MAGIC **Target schema** (`silver_returns`):
# MAGIC
# MAGIC | column | type |
# MAGIC |---|---|
# MAGIC | return_id | string |
# MAGIC | order_line_id | string |
# MAGIC | return_ts | timestamp |
# MAGIC | quantity | int |
# MAGIC | reason | string |
# MAGIC
# MAGIC **Example.** A return that points at a real line dated earlier than the return is kept; a return whose `order_line_id` does not exist in `silver_order_lines` (for example `OL-0-9999990`) is an orphan and goes to quarantine.
# MAGIC
# MAGIC **Definition of done.**
# MAGIC - Every row in `silver_returns` has an `order_line_id` that exists in `silver_order_lines`.
# MAGIC - Every row in `silver_returns` has `return_ts` strictly later than its line's `line_ts`.
# MAGIC - Orphan or wrongly dated returns are in `silver_returns_quarantine`, not in the clean table.
# MAGIC - The columns are typed as above.
# MAGIC
# MAGIC **References.** You may find these helpful:
# MAGIC - `3_3_silver_approach.md` in this folder, the approach doc for this lab: the write pattern for each feed shape, the quality rules and worked examples
# MAGIC - [Upsert into a Delta Lake table using merge](https://docs.databricks.com/aws/en/delta/merge)
# MAGIC - [Delta Lake table streaming reads and writes](https://docs.databricks.com/aws/en/structured-streaming/delta-lake)
# MAGIC - [Tutorial: Create and manage Delta Lake tables](https://docs.databricks.com/aws/en/delta/tutorial)

# COMMAND ----------

# Your turn. Build silver_returns (valid) and silver_returns_quarantine (orphans / bad dates).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Task 4: silver_returns: type returns and quarantine the rules breakers (solution)
# MAGIC <details>
# MAGIC <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; overflow:auto;"></pre>
# MAGIC
# MAGIC <script>
# MAGIC var codeB64 = "YnJvbnplID0gZiJ7Y2F0YWxvZ30uaGVsaW9zX2Jyb256ZSIKc2lsdmVyID0gZiJ7Y2F0YWxvZ30uaGVsaW9zX3NpbHZlciIKCiMgVGhlIGxpbmVzIGEgcmV0dXJuIGNhbiBsZWdhbGx5IHBvaW50IGF0LCB3aXRoIHRoZWlyIHRpbWVzdGFtcHMuCmxpbmVfcmVmID0gc3BhcmsucmVhZC50YWJsZShmIntzaWx2ZXJ9LnNpbHZlcl9vcmRlcl9saW5lcyIpLnNlbGVjdCgKICAgIGNvbCgib3JkZXJfbGluZV9pZCIpLmFsaWFzKCJfb2wiKSwgY29sKCJsaW5lX3RzIikuYWxpYXMoIl9saW5lX3RzIikpCgpyZXR1cm5zID0gKAogICAgc3BhcmsucmVhZC50YWJsZShmInticm9uemV9LmJyb256ZV9yZXR1cm5zIikKICAgIC5zZWxlY3QoCiAgICAgICAgY29sKCJyZXR1cm5faWQiKS5jYXN0KCJzdHJpbmciKS5hbGlhcygicmV0dXJuX2lkIiksCiAgICAgICAgY29sKCJvcmRlcl9saW5lX2lkIikuY2FzdCgic3RyaW5nIikuYWxpYXMoIm9yZGVyX2xpbmVfaWQiKSwKICAgICAgICBjb2woInJldHVybl90cyIpLmNhc3QoInRpbWVzdGFtcCIpLmFsaWFzKCJyZXR1cm5fdHMiKSwKICAgICAgICBjb2woInF1YW50aXR5IikuY2FzdCgiaW50IikuYWxpYXMoInF1YW50aXR5IiksCiAgICAgICAgY29sKCJyZWFzb24iKS5jYXN0KCJzdHJpbmciKS5hbGlhcygicmVhc29uIiksCiAgICApCiAgICAuam9pbihsaW5lX3JlZiwgY29sKCJvcmRlcl9saW5lX2lkIikgPT0gY29sKCJfb2wiKSwgImxlZnQiKQopCgojIFZhbGlkOiBwb2ludHMgYXQgYSByZWFsIGxpbmUgYW5kIGlzIGRhdGVkIGFmdGVyIGl0LiBFdmVyeXRoaW5nIGVsc2UgaXMgcXVhcmFudGluZWQuCmtlZXAgPSBjb2woIl9saW5lX3RzIikuaXNOb3ROdWxsKCkgJiAoY29sKCJyZXR1cm5fdHMiKSA+IGNvbCgiX2xpbmVfdHMiKSkKY29scyA9IFsicmV0dXJuX2lkIiwgIm9yZGVyX2xpbmVfaWQiLCAicmV0dXJuX3RzIiwgInF1YW50aXR5IiwgInJlYXNvbiJdCnVwc2VydChyZXR1cm5zLmZpbHRlcihrZWVwKS5zZWxlY3QoKmNvbHMpLCBmIntzaWx2ZXJ9LnNpbHZlcl9yZXR1cm5zIiwgInJldHVybl9pZCIpCnVwc2VydChyZXR1cm5zLmZpbHRlcih+a2VlcCkuc2VsZWN0KCpjb2xzKSwgZiJ7c2lsdmVyfS5zaWx2ZXJfcmV0dXJuc19xdWFyYW50aW5lIiwgInJldHVybl9pZCIp";
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
# MAGIC ### Self check: Task 4

# COMMAND ----------

silver = f"{catalog}.helios_silver"
returns = spark.table(f"{silver}.silver_returns")
lines = spark.table(f"{silver}.silver_order_lines")

check("return_ts is typed as a timestamp", "timestamp", dict(returns.dtypes)["return_ts"])
check("every return points at a real order line", 0,
      returns.select("order_line_id").join(lines.select("order_line_id"), "order_line_id", "left_anti").count())
joined = returns.alias("r").join(lines.alias("l"), col("r.order_line_id") == col("l.order_line_id"))
check("every return is dated after its line", 0, joined.filter("r.return_ts <= l.line_ts").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5: silver dimensions and inventory: overwrite the snapshots and build the movement log
# MAGIC
# MAGIC Build the four typed Silver reference dimensions (`silver_products`, `silver_categories`, `silver_suppliers`, `silver_warehouses`) by overwriting them, and build the typed `silver_inventory` movement log by upserting it on its movement key. Customers are not built here: they become an SCD Type 2 dimension in Lab 3.9. `3_3_silver_approach.md` covers why a full snapshot is overwritten while an append feed is upserted.
# MAGIC
# MAGIC **How.**
# MAGIC - The dimension feeds arrive as full snapshots: every drop is the complete current list, not a set of changes, so you overwrite rather than merge. Overwriting reflects anything removed at the source and is self-healing, whereas a merge only ever adds or updates.
# MAGIC - Read each Bronze table and type every column to the target schemas below; these are reference data, so keep one row per key.
# MAGIC - `inventory` is an append feed of stock movements, so type it and `upsert` it on its business key (`warehouse_id`, `product_id`, `movement_ts`), the same helper from Task 1 with a composite key.
# MAGIC
# MAGIC **Target schemas** (key columns and types):
# MAGIC
# MAGIC `silver_products`: product_id (string), sku (string), product_name (string), category_id (string), supplier_id (string), mass_kg (double), hazard_class (string), active (boolean).
# MAGIC
# MAGIC `silver_inventory`: warehouse_id (string), product_id (string), movement_ts (timestamp), delta (int), on_hand (int).
# MAGIC
# MAGIC (`silver_categories`, `silver_suppliers` and `silver_warehouses` are straight typed projections of their Bronze tables.)
# MAGIC
# MAGIC **Definition of done.**
# MAGIC - Each of `silver_products`, `silver_categories`, `silver_suppliers` and `silver_warehouses` has one row per key (no duplicates), built by `overwrite`.
# MAGIC - `silver_inventory` has one row per (`warehouse_id`, `product_id`, `movement_ts`), built by `upsert`.
# MAGIC - Types are as above (dates as `date`, `active` as `boolean`, `mass_kg` as `double`, `delta` and `on_hand` as `int`).
# MAGIC
# MAGIC **References.** You may find these helpful:
# MAGIC - `3_3_silver_approach.md` in this folder, the approach doc for this lab: the write pattern for each feed shape, the quality rules and worked examples
# MAGIC - [Upsert into a Delta Lake table using merge](https://docs.databricks.com/aws/en/delta/merge)
# MAGIC - [Tutorial: Create and manage Delta Lake tables](https://docs.databricks.com/aws/en/delta/tutorial)
# MAGIC - [Data modeling](https://docs.databricks.com/aws/en/transform/data-modeling)

# COMMAND ----------

# Your turn. Overwrite the five dimensions and upsert silver_inventory.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Task 5: silver dimensions and inventory: overwrite the snapshots and build the movement log (solution)
# MAGIC <details>
# MAGIC <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; overflow:auto;"></pre>
# MAGIC
# MAGIC <script>
# MAGIC var codeB64 = "YnJvbnplID0gZiJ7Y2F0YWxvZ30uaGVsaW9zX2Jyb256ZSIKc2lsdmVyID0gZiJ7Y2F0YWxvZ30uaGVsaW9zX3NpbHZlciIKCgpkZWYgb3ZlcndyaXRlKGRmLCB0YWJsZSk6CiAgICBkZi53cml0ZS5mb3JtYXQoImRlbHRhIikubW9kZSgib3ZlcndyaXRlIikub3B0aW9uKCJvdmVyd3JpdGVTY2hlbWEiLCAidHJ1ZSIpLnNhdmVBc1RhYmxlKHRhYmxlKQoKCiMgcHJvZHVjdHM6IHJlZmVyZW5jZSBkYXRhLCBvbmUgcm93IHBlciBwcm9kdWN0X2lkLCB0eXBlZC4Kb3ZlcndyaXRlKAogICAgc3BhcmsucmVhZC50YWJsZShmInticm9uemV9LmJyb256ZV9wcm9kdWN0cyIpLnNlbGVjdCgKICAgICAgICBjb2woInByb2R1Y3RfaWQiKS5jYXN0KCJzdHJpbmciKS5hbGlhcygicHJvZHVjdF9pZCIpLAogICAgICAgIGNvbCgic2t1IikuY2FzdCgic3RyaW5nIikuYWxpYXMoInNrdSIpLAogICAgICAgIGNvbCgicHJvZHVjdF9uYW1lIikuY2FzdCgic3RyaW5nIikuYWxpYXMoInByb2R1Y3RfbmFtZSIpLAogICAgICAgIGNvbCgiY2F0ZWdvcnlfaWQiKS5jYXN0KCJzdHJpbmciKS5hbGlhcygiY2F0ZWdvcnlfaWQiKSwKICAgICAgICBjb2woInN1cHBsaWVyX2lkIikuY2FzdCgic3RyaW5nIikuYWxpYXMoInN1cHBsaWVyX2lkIiksCiAgICAgICAgY29sKCJtYXNzX2tnIikuY2FzdCgiZG91YmxlIikuYWxpYXMoIm1hc3Nfa2ciKSwKICAgICAgICBjb2woImhhemFyZF9jbGFzcyIpLmNhc3QoInN0cmluZyIpLmFsaWFzKCJoYXphcmRfY2xhc3MiKSwKICAgICAgICBjb2woImFjdGl2ZSIpLmNhc3QoImJvb2xlYW4iKS5hbGlhcygiYWN0aXZlIiksCiAgICApLmRyb3BEdXBsaWNhdGVzKFsicHJvZHVjdF9pZCJdKSwKICAgIGYie3NpbHZlcn0uc2lsdmVyX3Byb2R1Y3RzIikKCm92ZXJ3cml0ZSgKICAgIHNwYXJrLnJlYWQudGFibGUoZiJ7YnJvbnplfS5icm9uemVfY2F0ZWdvcmllcyIpLnNlbGVjdCgKICAgICAgICBjb2woImNhdGVnb3J5X2lkIikuY2FzdCgic3RyaW5nIikuYWxpYXMoImNhdGVnb3J5X2lkIiksCiAgICAgICAgY29sKCJjYXRlZ29yeV9uYW1lIikuY2FzdCgic3RyaW5nIikuYWxpYXMoImNhdGVnb3J5X25hbWUiKSwKICAgICAgICBjb2woImRlcGFydG1lbnQiKS5jYXN0KCJzdHJpbmciKS5hbGlhcygiZGVwYXJ0bWVudCIpLAogICAgKS5kcm9wRHVwbGljYXRlcyhbImNhdGVnb3J5X2lkIl0pLAogICAgZiJ7c2lsdmVyfS5zaWx2ZXJfY2F0ZWdvcmllcyIpCgpvdmVyd3JpdGUoCiAgICBzcGFyay5yZWFkLnRhYmxlKGYie2Jyb256ZX0uYnJvbnplX3N1cHBsaWVycyIpLnNlbGVjdCgKICAgICAgICBjb2woInN1cHBsaWVyX2lkIikuY2FzdCgic3RyaW5nIikuYWxpYXMoInN1cHBsaWVyX2lkIiksCiAgICAgICAgY29sKCJzdXBwbGllcl9uYW1lIikuY2FzdCgic3RyaW5nIikuYWxpYXMoInN1cHBsaWVyX25hbWUiKSwKICAgICAgICBjb2woImhvbWVfcmVnaW9uIikuY2FzdCgic3RyaW5nIikuYWxpYXMoImhvbWVfcmVnaW9uIiksCiAgICAgICAgY29sKCJhY3RpdmUiKS5jYXN0KCJib29sZWFuIikuYWxpYXMoImFjdGl2ZSIpLAogICAgKS5kcm9wRHVwbGljYXRlcyhbInN1cHBsaWVyX2lkIl0pLAogICAgZiJ7c2lsdmVyfS5zaWx2ZXJfc3VwcGxpZXJzIikKCm92ZXJ3cml0ZSgKICAgIHNwYXJrLnJlYWQudGFibGUoZiJ7YnJvbnplfS5icm9uemVfd2FyZWhvdXNlcyIpLnNlbGVjdCgKICAgICAgICBjb2woIndhcmVob3VzZV9pZCIpLmNhc3QoInN0cmluZyIpLmFsaWFzKCJ3YXJlaG91c2VfaWQiKSwKICAgICAgICBjb2woIndhcmVob3VzZV9uYW1lIikuY2FzdCgic3RyaW5nIikuYWxpYXMoIndhcmVob3VzZV9uYW1lIiksCiAgICAgICAgY29sKCJib2R5IikuY2FzdCgic3RyaW5nIikuYWxpYXMoImJvZHkiKSwKICAgICAgICBjb2woInJlZ2lvbiIpLmNhc3QoInN0cmluZyIpLmFsaWFzKCJyZWdpb24iKSwKICAgICAgICBjb2woInVwbGlua19yZWxpYWJpbGl0eSIpLmNhc3QoImRvdWJsZSIpLmFsaWFzKCJ1cGxpbmtfcmVsaWFiaWxpdHkiKSwKICAgICkuZHJvcER1cGxpY2F0ZXMoWyJ3YXJlaG91c2VfaWQiXSksCiAgICBmIntzaWx2ZXJ9LnNpbHZlcl93YXJlaG91c2VzIikKCiMgaW52ZW50b3J5OiBhcHBlbmQgbW92ZW1lbnQgbG9nLCB1cHNlcnRlZCBvbiBpdHMgY29tcG9zaXRlIGJ1c2luZXNzIGtleS4KaW52ZW50b3J5ID0gKAogICAgc3BhcmsucmVhZC50YWJsZShmInticm9uemV9LmJyb256ZV9pbnZlbnRvcnkiKQogICAgLnNlbGVjdCgKICAgICAgICBjb2woIndhcmVob3VzZV9pZCIpLmNhc3QoInN0cmluZyIpLmFsaWFzKCJ3YXJlaG91c2VfaWQiKSwKICAgICAgICBjb2woInByb2R1Y3RfaWQiKS5jYXN0KCJzdHJpbmciKS5hbGlhcygicHJvZHVjdF9pZCIpLAogICAgICAgIGNvbCgibW92ZW1lbnRfdHMiKS5jYXN0KCJ0aW1lc3RhbXAiKS5hbGlhcygibW92ZW1lbnRfdHMiKSwKICAgICAgICBjb2woImRlbHRhIikuY2FzdCgiaW50IikuYWxpYXMoImRlbHRhIiksCiAgICAgICAgY29sKCJvbl9oYW5kIikuY2FzdCgiaW50IikuYWxpYXMoIm9uX2hhbmQiKSwKICAgICkKICAgIC5kcm9wRHVwbGljYXRlcyhbIndhcmVob3VzZV9pZCIsICJwcm9kdWN0X2lkIiwgIm1vdmVtZW50X3RzIl0pCikKdXBzZXJ0KGludmVudG9yeSwgZiJ7c2lsdmVyfS5zaWx2ZXJfaW52ZW50b3J5IiwgWyJ3YXJlaG91c2VfaWQiLCAicHJvZHVjdF9pZCIsICJtb3ZlbWVudF90cyJdKQ==";
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
# MAGIC ### Self check: Task 5

# COMMAND ----------

silver = f"{catalog}.helios_silver"
products = spark.table(f"{silver}.silver_products")
inventory = spark.table(f"{silver}.silver_inventory")

for name, key in [("silver_products", "product_id"), ("silver_categories", "category_id"),
                  ("silver_suppliers", "supplier_id"), ("silver_warehouses", "warehouse_id")]:
    t = spark.table(f"{silver}.{name}")
    check(f"{name} has one row per {key}", 0, t.groupBy(key).count().filter("count > 1").count())

check("active is typed as a boolean", "boolean", dict(products.dtypes)["active"])
check("silver_inventory has one row per (warehouse, product, movement_ts)", 0,
      inventory.groupBy("warehouse_id", "product_id", "movement_ts").count().filter("count > 1").count())
check("on_hand is typed as an int", "int", dict(inventory.dtypes)["on_hand"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## What you built
# MAGIC
# MAGIC A trustworthy Silver layer in the DataFrame and Delta APIs: a tested `upsert` helper, an order line stream that is typed, deduped and quarantined, the order CDC feed reduced to current state, returns checked against their lines with orphans quarantined, the four reference dimensions overwritten from the latest snapshot, and the inventory movement log upserted on its key. Customers become an SCD Type 2 dimension in Lab 3.9. Upsert the append and CDC feeds, overwrite the full snapshots.