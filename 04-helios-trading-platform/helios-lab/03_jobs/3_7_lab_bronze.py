# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 3.7: Bronze: Auto Loader from the Volume
# MAGIC
# MAGIC ### Ticket: HELIOS-307
# MAGIC
# MAGIC **Context.** The contract is agreed and batch one is landed in your Volume. Time to build the first layer of the medallion the classic way, with Databricks Jobs. Bronze is the raw landing zone inside the lakehouse: every source file ingested as-is, with just enough metadata to trace each row back to its file. No business logic yet, that is Silver's job.
# MAGIC
# MAGIC **Tasks.**
# MAGIC 1. `bronze_order_lines`: ingest the hero order line feed with Auto Loader.
# MAGIC 2. `bronze_*`: ingest the other eight feeds the same way.
# MAGIC
# MAGIC **Acceptance criteria** (the self checks): 
# MAGIC - all nine `bronze_*` tables exist and mirror their landing folders row for row; 
# MAGIC - every table carries `_rescued_data`, `_ingest_ts` and `_source_file`; 
# MAGIC - every `bronze_order_lines` row is traceable to its source file. (`returns` arrives from batch two, so it is not ingested in this lab.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bootstrap
# MAGIC Starts from a clean slate so the lab is deterministic on every run: reset the Bronze schema (Bronze is the first layer you build, so there is no upstream Delta to restore), clear the landing Volume, and re-land batch one. Quick at this scale.

# COMMAND ----------

# MAGIC %run ../00_setup/data_generator

# COMMAND ----------

# MAGIC %run ../00_setup/bootstrap_helpers

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# Clean slate: reset the layer this lab builds (Bronze) and re-land batch one, so the lab is
# deterministic on every run. Resetting a schema also clears its checkpoints Volume.
catalog = helios_identity()
reset_schema(catalog, "helios_bronze")
clear_landing(catalog)
ensure_landing(catalog, up_to_batch=1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: bronze_order_lines: ingest the order line feed with Auto Loader
# MAGIC
# MAGIC Ingest the raw `order_lines` JSON feed into a `bronze_order_lines` table with Auto Loader, landing every row exactly as it arrived and adding ingest metadata so each row is traceable to its source file. Every Auto Loader setting used here is explained with worked examples in `3_2_auto_loader.md`, in this folder.
# MAGIC
# MAGIC **How.**
# MAGIC - Bronze is the raw landing zone inside the lakehouse: a durable, traceable copy of the source with no business logic yet (that is Silver's job).
# MAGIC - Read the feed with Auto Loader (`cloudFiles`), which keeps a checkpoint of the files it has already read and loads only new ones on each run.
# MAGIC - Auto Loader needs somewhere to keep that checkpoint and its inferred schema, so create a checkpoints Volume and point it at a path. Use a generic location like `/Volumes/<catalog>/helios_bronze/checkpoints`, with a subfolder per feed.
# MAGIC - Keep a rescued-data column, so any value that does not match the inferred schema is preserved rather than silently dropped.
# MAGIC - A source schema can drift over time as an upstream system adds a column, so configure Auto Loader to absorb a new column rather than fail (the `addNewColumns` mode), and let the Bronze table add it on write. This dataset's schema is stable, but Bronze is built to handle drift from the start, which is standard defensive practice.
# MAGIC - Add the ingest metadata: when each row was loaded, and which file it came from.
# MAGIC - This runs as a scheduled **batch** Job, not a 24/7 stream, so trigger it to process whatever has currently landed and then stop (an "available now" run), and make the cell wait for that run to finish before the next step reads the table.
# MAGIC
# MAGIC **Columns added beyond the source.**
# MAGIC
# MAGIC | column | type |
# MAGIC |---|---|
# MAGIC | _rescued_data | string |
# MAGIC | _ingest_ts | timestamp |
# MAGIC | _source_file | string |
# MAGIC
# MAGIC **Example.** Batch one lands 50,626 rows. Because the clean batch matches the schema, `_rescued_data` is null, `_source_file` points at `.../order_lines/batch_1/part-....json`, and `_ingest_ts` is the load time.
# MAGIC
# MAGIC **Definition of done.**
# MAGIC - `bronze_order_lines` exists and holds every row that landed in `order_lines/` (50,626 on batch one), ingestion neither dropped nor duplicated any row.
# MAGIC - It carries `_rescued_data`, `_ingest_ts` and `_source_file`.
# MAGIC - Every row has a non-null `_source_file`.
# MAGIC - Re-running ingests no duplicates (Auto Loader's checkpoint skips files it has already read).
# MAGIC
# MAGIC **References.** You may find these helpful:
# MAGIC - `3_2_auto_loader.md` in this folder, the approach doc for this lab: how the files arrive, every Auto Loader setting used here, and worked examples
# MAGIC - [What is Auto Loader?](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)
# MAGIC - [Configure schema inference and evolution in Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema)
# MAGIC - [File metadata column](https://docs.databricks.com/aws/en/ingestion/file-metadata-column)
# MAGIC - [Configure Structured Streaming trigger intervals](https://docs.databricks.com/aws/en/structured-streaming/triggers)

# COMMAND ----------

# Your turn. Ingest order_lines with Auto Loader into bronze_order_lines.
# (Import what you use from pyspark.sql.functions.)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Task 1: bronze_order_lines: ingest the order line feed with Auto Loader (solution)
# MAGIC <details>
# MAGIC <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; overflow:auto;"></pre>
# MAGIC
# MAGIC <script>
# MAGIC var codeB64 = "IyBBdXRvIExvYWRlciBrZWVwcyBpdHMgZmlsZS10cmFja2luZyBzdGF0ZSBpbiBhIGNoZWNrcG9pbnQsIHNvIGEgcmUtcnVuIG9ubHkgaW5nZXN0cyBuZXcgZmlsZXMuCnNwYXJrLnNxbChmIkNSRUFURSBWT0xVTUUgSUYgTk9UIEVYSVNUUyB7Y2F0YWxvZ30uaGVsaW9zX2Jyb256ZS5jaGVja3BvaW50cyIpCmNoayA9IGYiL1ZvbHVtZXMve2NhdGFsb2d9L2hlbGlvc19icm9uemUvY2hlY2twb2ludHMiCnJvb3QgPSBmIi9Wb2x1bWVzL3tjYXRhbG9nfS9oZWxpb3NfcmF3L2hlbGlvc19sYW5kaW5nIgoKc3RyZWFtID0gKAogICAgc3BhcmsucmVhZFN0cmVhbS5mb3JtYXQoImNsb3VkRmlsZXMiKQogICAgLm9wdGlvbigiY2xvdWRGaWxlcy5mb3JtYXQiLCAianNvbiIpCiAgICAub3B0aW9uKCJjbG91ZEZpbGVzLnNjaGVtYUxvY2F0aW9uIiwgZiJ7Y2hrfS9vcmRlcl9saW5lcy9zY2hlbWEiKQogICAgLm9wdGlvbigiY2xvdWRGaWxlcy5zY2hlbWFFdm9sdXRpb25Nb2RlIiwgImFkZE5ld0NvbHVtbnMiKSAgICMgYWJzb3JiIGEgbmV3IHVwc3RyZWFtIGNvbHVtbiByYXRoZXIgdGhhbiBmYWlsCiAgICAub3B0aW9uKCJyZXNjdWVkRGF0YUNvbHVtbiIsICJfcmVzY3VlZF9kYXRhIikgICAgICAgICAgICAgICAgIyBrZWVwIGFueXRoaW5nIHRoYXQgZG9lcyBub3QgbWF0Y2ggdGhlIHNjaGVtYQogICAgLmxvYWQoZiJ7cm9vdH0vb3JkZXJfbGluZXMiKQogICAgLndpdGhDb2x1bW4oIl9pbmdlc3RfdHMiLCBjdXJyZW50X3RpbWVzdGFtcCgpKQogICAgLndpdGhDb2x1bW4oIl9zb3VyY2VfZmlsZSIsIGNvbCgiX21ldGFkYXRhLmZpbGVfcGF0aCIpKQopCgpxdWVyeSA9ICgKICAgIHN0cmVhbS53cml0ZVN0cmVhbQogICAgLm9wdGlvbigiY2hlY2twb2ludExvY2F0aW9uIiwgZiJ7Y2hrfS9vcmRlcl9saW5lcy9jaGsiKQogICAgLm9wdGlvbigibWVyZ2VTY2hlbWEiLCAidHJ1ZSIpICAgICAjIGxldCBCcm9uemUgYWJzb3JiIGEgbmV3IHNvdXJjZSBjb2x1bW4gaWYgdGhlIHNjaGVtYSBldmVyIGRyaWZ0cwogICAgLnRyaWdnZXIoYXZhaWxhYmxlTm93PVRydWUpICAgICAgICAjIHByb2Nlc3Mgd2hhdCBoYXMgbGFuZGVkLCB0aGVuIHN0b3AgKHRoaXMgcnVucyBhcyBhIGJhdGNoIEpvYikKICAgIC50b1RhYmxlKGYie2NhdGFsb2d9LmhlbGlvc19icm9uemUuYnJvbnplX29yZGVyX2xpbmVzIikKKQpxdWVyeS5hd2FpdFRlcm1pbmF0aW9uKCkgICAgICAgICAgICAgICAjIGJsb2NrIHRoaXMgY2VsbCB1bnRpbCBpbmdlc3Rpb24gaGFzIGZpbmlzaGVk";
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

bronze = f"{catalog}.helios_bronze"
root = f"/Volumes/{catalog}/helios_raw/helios_landing"


def landed_count(feed, fmt):
    """Rows currently sitting in a feed's landing folder, across whatever batches have landed."""
    reader = spark.read.option("recursiveFileLookup", "true")
    if fmt == "csv":
        reader = reader.option("header", "true")
    return reader.format(fmt).load(f"{root}/{feed}").count()


bronze_lines = spark.table(f"{bronze}.bronze_order_lines")
# Bronze's job is to mirror the source: ingest every landed row, drop or duplicate none. Comparing to the
# landing folder holds however many batches have landed, where a hardcoded 50,626 would break on batch two.
check("bronze_order_lines ingested every landed row", landed_count("order_lines", "json"), bronze_lines.count())
check_true("ingest metadata columns were added",
           {"_rescued_data", "_ingest_ts", "_source_file"}.issubset(set(bronze_lines.columns)))
check("every row is traceable to a source file", 0, bronze_lines.filter("_source_file IS NULL").count())
check("nothing landed in the rescued-data column", 0, bronze_lines.filter("_rescued_data IS NOT NULL").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: bronze: ingest the other eight feeds
# MAGIC
# MAGIC Ingest the other eight source feeds into their own `bronze_*` tables the same way you did the order lines, so Bronze captures every source. The format per feed and the checkpoint layout are set out in `3_2_auto_loader.md`.
# MAGIC
# MAGIC **How.**
# MAGIC - Use the same Auto Loader pattern for each feed, picking the right format: JSON for `orders`, `inventory`, `suppliers` and `warehouses`; Parquet for `customers`, `products` and `categories`; and CSV with a header row for `price_list`.
# MAGIC - Keep each feed's checkpoint and schema under the same `/Volumes/<catalog>/helios_bronze/checkpoints` Volume, in its own subfolder named for the feed, so the feeds do not collide.
# MAGIC - Each lands its raw columns plus the same three metadata columns, as an "available now" run that processes what has landed and then stops.
# MAGIC
# MAGIC **Example.** Each feed becomes one Bronze table:
# MAGIC
# MAGIC | source feed | format | bronze table |
# MAGIC |---|---|---|
# MAGIC | orders | JSON | `bronze_orders` |
# MAGIC | inventory | JSON | `bronze_inventory` |
# MAGIC | price_list | CSV (header) | `bronze_price_list` |
# MAGIC | customers | Parquet | `bronze_customers` |
# MAGIC | products | Parquet | `bronze_products` |
# MAGIC | categories | Parquet | `bronze_categories` |
# MAGIC | suppliers | JSON | `bronze_suppliers` |
# MAGIC | warehouses | JSON | `bronze_warehouses` |
# MAGIC
# MAGIC **Definition of done.**
# MAGIC - All nine `bronze_*` tables exist in `helios_bronze` (the eight above plus `bronze_order_lines`).
# MAGIC - Each Bronze table holds exactly the rows that landed for it, ingestion neither dropped nor duplicated any row (orders 45,628, inventory 46,575, price_list 150, customers 300, products 150, categories 7, suppliers 80, warehouses 6 on batch one).
# MAGIC - Every Bronze table carries `_rescued_data`, `_ingest_ts` and `_source_file`.
# MAGIC
# MAGIC **References.** You may find these helpful:
# MAGIC - `3_2_auto_loader.md` in this folder, the approach doc for this lab: how the files arrive, every Auto Loader setting used here, and worked examples
# MAGIC - [What is Auto Loader?](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)
# MAGIC - [Spark API options reference](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/options)
# MAGIC - [File metadata column](https://docs.databricks.com/aws/en/ingestion/file-metadata-column)
# MAGIC - [Configure Structured Streaming trigger intervals](https://docs.databricks.com/aws/en/structured-streaming/triggers)

# COMMAND ----------

# Your turn. Ingest the remaining eight feeds into their bronze_ tables.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Task 2: bronze: ingest the other eight feeds (solution)
# MAGIC <details>
# MAGIC <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; overflow:auto;"></pre>
# MAGIC
# MAGIC <script>
# MAGIC var codeB64 = "Y2hrID0gZiIvVm9sdW1lcy97Y2F0YWxvZ30vaGVsaW9zX2Jyb256ZS9jaGVja3BvaW50cyIKcm9vdCA9IGYiL1ZvbHVtZXMve2NhdGFsb2d9L2hlbGlvc19yYXcvaGVsaW9zX2xhbmRpbmciCgoKZGVmIGF1dG9sb2FkKG5hbWUsIGZtdCk6CiAgICAiIiJJbmdlc3Qgb25lIGZlZWQgaW50byBicm9uemVfPG5hbWU+IHdpdGggdGhlIHNhbWUgQXV0byBMb2FkZXIgcGF0dGVybiBhcyB0aGUgb3JkZXIgbGluZXMuIiIiCiAgICByZWFkZXIgPSAoCiAgICAgICAgc3BhcmsucmVhZFN0cmVhbS5mb3JtYXQoImNsb3VkRmlsZXMiKQogICAgICAgIC5vcHRpb24oImNsb3VkRmlsZXMuZm9ybWF0IiwgZm10KQogICAgICAgIC5vcHRpb24oImNsb3VkRmlsZXMuc2NoZW1hTG9jYXRpb24iLCBmIntjaGt9L3tuYW1lfS9zY2hlbWEiKQogICAgICAgIC5vcHRpb24oImNsb3VkRmlsZXMuc2NoZW1hRXZvbHV0aW9uTW9kZSIsICJhZGROZXdDb2x1bW5zIikKICAgICAgICAub3B0aW9uKCJyZXNjdWVkRGF0YUNvbHVtbiIsICJfcmVzY3VlZF9kYXRhIikKICAgICkKICAgIGlmIGZtdCA9PSAiY3N2IjoKICAgICAgICByZWFkZXIgPSByZWFkZXIub3B0aW9uKCJoZWFkZXIiLCAidHJ1ZSIpCiAgICBxdWVyeSA9ICgKICAgICAgICByZWFkZXIubG9hZChmIntyb290fS97bmFtZX0iKQogICAgICAgIC53aXRoQ29sdW1uKCJfaW5nZXN0X3RzIiwgY3VycmVudF90aW1lc3RhbXAoKSkKICAgICAgICAud2l0aENvbHVtbigiX3NvdXJjZV9maWxlIiwgY29sKCJfbWV0YWRhdGEuZmlsZV9wYXRoIikpCiAgICAgICAgLndyaXRlU3RyZWFtCiAgICAgICAgLm9wdGlvbigiY2hlY2twb2ludExvY2F0aW9uIiwgZiJ7Y2hrfS97bmFtZX0vY2hrIikKICAgICAgICAub3B0aW9uKCJtZXJnZVNjaGVtYSIsICJ0cnVlIikKICAgICAgICAudHJpZ2dlcihhdmFpbGFibGVOb3c9VHJ1ZSkKICAgICAgICAudG9UYWJsZShmIntjYXRhbG9nfS5oZWxpb3NfYnJvbnplLmJyb256ZV97bmFtZX0iKQogICAgKQogICAgcXVlcnkuYXdhaXRUZXJtaW5hdGlvbigpCgoKZm9yIG5hbWUsIGZtdCBpbiBbKCJvcmRlcnMiLCAianNvbiIpLCAoImludmVudG9yeSIsICJqc29uIiksICgicHJpY2VfbGlzdCIsICJjc3YiKSwgKCJjdXN0b21lcnMiLCAicGFycXVldCIpLAogICAgICAgICAgICAgICAgICAoInByb2R1Y3RzIiwgInBhcnF1ZXQiKSwgKCJjYXRlZ29yaWVzIiwgInBhcnF1ZXQiKSwgKCJzdXBwbGllcnMiLCAianNvbiIpLCAoIndhcmVob3VzZXMiLCAianNvbiIpXToKICAgIGF1dG9sb2FkKG5hbWUsIGZtdCk=";
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

formats = {"order_lines": "json", "orders": "json", "inventory": "json", "price_list": "csv",
           "customers": "parquet", "products": "parquet", "categories": "parquet",
           "suppliers": "json", "warehouses": "json"}

missing = {name for name in formats if not spark.catalog.tableExists(f"{bronze}.bronze_{name}")}
check("all nine bronze tables exist", set(), missing)

# Each Bronze table mirrors its landing folder row for row (ingestion neither dropped nor duplicated rows).
landed = {name: landed_count(name, fmt) for name, fmt in formats.items()}
ingested = {name: spark.table(f"{bronze}.bronze_{name}").count() for name in formats}
check("every bronze table mirrors its landing folder", landed, ingested)

metadata = {"_rescued_data", "_ingest_ts", "_source_file"}
without_metadata = {name for name in formats
                    if not metadata.issubset(set(spark.table(f"{bronze}.bronze_{name}").columns))}
check("every bronze table carries the ingest metadata", set(), without_metadata)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What you built
# MAGIC
# MAGIC Nine raw Bronze tables, ingested incrementally with Auto Loader, each row traceable to its source file and nothing silently dropped. This is the durable raw layer every later step reads from. (`returns` joins them from batch two.)