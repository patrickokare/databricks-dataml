# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 5.3: Build the semantic layer with Metric Views
# MAGIC
# MAGIC ### Ticket: HELIOS-503
# MAGIC
# MAGIC **Context.** Section 5 turns the canonical Gold into self-serve insight. The plan is one semantic layer that both surfaces read: Genie answers questions in plain English and the AI/BI dashboards draw the charts, both sitting on the same metric views, so a metric defined once means the same thing everywhere and a fix propagates to both. In this lab you build that layer in `helios_semantic` over Gold, as three combos. Each combo is a plain view (named `_vw`) that flattens the data, and a metric view (named `_mv`) that defines the business metrics on it.
# MAGIC
# MAGIC **Task.**
# MAGIC 1. Comment and certify the Gold star so it is discoverable and the metric layer and Genie can trust it.
# MAGIC 2. Build the sales combo: the view `order_lines_vw` and the metric view `sales_mv` (revenue, margin, units, average order value, returns).
# MAGIC 3. Build the orders combo: `orders_vw` and `orders_mv` (cancellation rate, on-time fulfilment).
# MAGIC 4. Build the inventory combo: `inventory_vw` and `inventory_mv` (stock movements and stockouts).
# MAGIC
# MAGIC **Acceptance criteria** (the self check cells):
# MAGIC - Every Gold fact and dimension has a table comment, and the key business columns are commented.
# MAGIC - `sales_mv` returns the right totals (revenue, units, average order value) and isolates the incident: lines sold below cost carry a negative margin.
# MAGIC - `orders_mv` returns the cancellation rate and the on-time fulfilment rate.
# MAGIC - `inventory_mv` shows the Ares stockout (Min On Hand 0).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bootstrap
# MAGIC Rebuilds the canonical Gold from the Section 3 production task notebooks, then creates an empty `helios_semantic` schema for you to build into. It lands the full simulated period (batches one to five), not just the incident batch, because the consumption layer should show the whole story: the incident on 2257-03-03 and its aftermath (the cancellation wave, the return wave and the customer tier promotions). Run it top to bottom before the tasks. It is idempotent, so it is safe to re-run.

# COMMAND ----------

# MAGIC %run ../00_setup/data_generator

# COMMAND ----------

# MAGIC %run ../00_setup/bootstrap_helpers

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

gold = f"{catalog}.helios_gold"
print("Canonical Gold rebuilt for", catalog)
print("fact_order_lines rows:", spark.table(f"{gold}.fact_order_lines").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your task: Build the semantic layer
# MAGIC
# MAGIC You will build three combos in `helios_semantic`, plus the comments that make Gold trustworthy. Write each task in the empty cell under it, then run the self check below it. The reveal has the solution if you get stuck.
# MAGIC
# MAGIC **What a metric view is, in plain terms.** A normal view returns rows. A metric view instead stores a set of named **dimensions** (the columns you are allowed to group by) and named **measures** (aggregates such as `SUM(...)`, written once). You do not run `SELECT *` on it. You ask for a measure with the `MEASURE(...)` function and add `GROUP BY ALL`, and the engine computes the aggregate at the grain you asked for. Define `Revenue` as `SUM(gross_amount)` once, and it stays correct whether you look at revenue for the whole company, by depot, or by depot and day, because the aggregation happens at query time, not when the view is built. That one definition is what Genie and the dashboards both read.
# MAGIC
# MAGIC **Why a plain view first.** A metric view reads from a single source. Our facts need columns from several dimensions (a depot name, a category, a customer tier), so each combo first builds a `_vw` view that joins the fact to its dimensions into one flat row, and the `_mv` metric view then sources `SELECT * FROM` that view. It keeps each metric view simple and means a join is fixed in one place.
# MAGIC
# MAGIC **A note on the tools.** There is no DataFrame API for creating or querying a metric view, so this lab uses `spark.sql` for the view and metric view DDL and for the `MEASURE` reads in the self checks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Comment and certify the Gold star schema
# MAGIC
# MAGIC Make the Gold tables discoverable and trustworthy by adding table and column comments, because Genie reads those comments as context when it writes SQL, and the dashboards and your teammates read them in Catalog Explorer. Good comments are the cheapest, highest-leverage thing you can do for answer quality.
# MAGIC
# MAGIC **How.**
# MAGIC - Comment each Gold fact and dimension at the table level (its grain and purpose), and comment the key columns the business slices and measures on: `margin`, `gross_amount`, `unit_price`, `unit_cost`, `warehouse_id`, `product_id` and `is_returned` on `fact_order_lines`, `tier` on `dim_customer`, `region` on `dim_warehouse`, and `on_hand` on `fact_inventory`.
# MAGIC - Keep each comment generic: say what the column means and name the units (CREDITS) for a money column, and list the value domain where it helps (for example the tier or region values). Do not bake specific ids or the incident story into a column comment; that context belongs in the Genie space instructions, not in the schema.
# MAGIC - You have two ways to add the comments, and either satisfies the self check:
# MAGIC   - Write them yourself with `COMMENT ON TABLE` and `COMMENT ON COLUMN`. There is no DataFrame API for comments, so call them with `spark.sql`. The reveal shows this.
# MAGIC   - Or let Databricks generate them: in Catalog Explorer, open each table and use the AI-suggested comments, then review and edit every one before you accept it. Treat the generated text as a draft; the review is the point, since a wrong or vague comment misleads Genie.
# MAGIC - Certification is the matching governance step: in Catalog Explorer you can mark these tables Certified and add tags, so consumers know they are the trusted source.
# MAGIC
# MAGIC **Example.** `COMMENT ON COLUMN ...fact_order_lines.margin IS 'Line gross profit: revenue minus the point in time cost, in CREDITS. Negative when the line sold below cost.'`
# MAGIC
# MAGIC **Definition of done.**
# MAGIC - Each of the three facts and the seven dimensions in `helios_gold` has a non-empty table comment.
# MAGIC - The key business columns are commented, including `fact_order_lines.margin`, `gross_amount` and `warehouse_id`, `dim_customer.tier`, and `fact_inventory.on_hand`.
# MAGIC
# MAGIC **References.** You may find these Databricks docs helpful:
# MAGIC - [COMMENT ON](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-comment)
# MAGIC - [Add AI-generated comments to Unity Catalog objects](https://docs.databricks.com/aws/en/comments/ai-comments)
# MAGIC - [Apply tags to Unity Catalog securable objects](https://docs.databricks.com/aws/en/database-objects/tags)
# MAGIC - [Flag data as certified or deprecated](https://docs.databricks.com/aws/en/data-governance/unity-catalog/certify-deprecate-data)

# COMMAND ----------

# Your turn. Comment the Gold tables and columns, then run the self check below.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Task 1: Comment and certify the Gold star schema (solution)
# MAGIC <details>
# MAGIC <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; overflow:auto;"></pre>
# MAGIC
# MAGIC <script>
# MAGIC var codeB64 = "IyBUYWJsZSBjb21tZW50czogc3RhdGUgZWFjaCB0YWJsZSdzIGdyYWluIGFuZCBwdXJwb3NlIGluIHBsYWluIGJ1c2luZXNzIGxhbmd1YWdlLgpzcGFyay5zcWwoZiJDT01NRU5UIE9OIFRBQkxFIHtjYXRhbG9nfS5oZWxpb3NfZ29sZC5mYWN0X29yZGVyX2xpbmVzIElTICdPcmRlciBsaW5lIGZhY3QsIG9uZSByb3cgcGVyIHBsYWNlZCBvcmRlciBsaW5lLiBBIGJvb2tpbmdzIGdyYWluOiBpdCBrZWVwcyBsaW5lcyBmcm9tIGNhbmNlbGxlZCBhbmQgaW4gZmxpZ2h0IG9yZGVycywgc28gcmV2ZW51ZSBhbmQgdW5pdHMgYXJlIGdyb3NzIGJvb2tpbmdzLiBQb2ludCBpbiB0aW1lIHByaWNlZCBhbmQgY29zdGVkOyB0aGUgaGVybyBmYWN0IGZvciByZXZlbnVlIGFuZCBtYXJnaW4uIEV4Y2x1ZGUgQ0FOQ0VMTEVEIG9yZGVyX3N0YXR1cyBmb3IgbmV0LW9mLWNhbmNlbGxhdGlvbiByZXZlbnVlLiciKQpzcGFyay5zcWwoZiJDT01NRU5UIE9OIFRBQkxFIHtjYXRhbG9nfS5oZWxpb3NfZ29sZC5mYWN0X3JldHVybnMgICAgSVMgJ1JldHVybiBmYWN0LCBvbmUgcm93IHBlciByZXR1cm4sIHJlZnVuZGVkIGF0IHRoZSBwcmljZSB0aGUgY3VzdG9tZXIgb3JpZ2luYWxseSBwYWlkIChwb2ludCBpbiB0aW1lIGF0IHRoZSBsaW5lKS4nIikKc3Bhcmsuc3FsKGYiQ09NTUVOVCBPTiBUQUJMRSB7Y2F0YWxvZ30uaGVsaW9zX2dvbGQuZmFjdF9pbnZlbnRvcnkgIElTICdTdG9jayBtb3ZlbWVudCBmYWN0LCBvbmUgcm93IHBlciBtb3ZlbWVudCwgY2FycnlpbmcgdGhlIHNpZ25lZCBkZWx0YSBhbmQgdGhlIHJ1bm5pbmcgb25faGFuZCBiYWxhbmNlIHBlciBkZXBvdCBhbmQgcHJvZHVjdC4nIikKc3Bhcmsuc3FsKGYiQ09NTUVOVCBPTiBUQUJMRSB7Y2F0YWxvZ30uaGVsaW9zX2dvbGQuZGltX3dhcmVob3VzZSAgIElTICdEZXBvdCBkaW1lbnNpb24uIFNpeCBkZXBvdHMgYWNyb3NzIHRoZSBzb2xhciBzeXN0ZW0sIHRoZSBzbGljaW5nIGF4aXMgZm9yIGV2ZXJ5IGRhc2hib2FyZC4nIikKc3Bhcmsuc3FsKGYiQ09NTUVOVCBPTiBUQUJMRSB7Y2F0YWxvZ30uaGVsaW9zX2dvbGQuZGltX3Byb2R1Y3QgICAgIElTICdQcm9kdWN0IGRpbWVuc2lvbi4gMTUwIHNoaXAgcGFydHMsIGVhY2ggaW4gb25lIGNhdGVnb3J5IGFuZCBmcm9tIG9uZSBzdXBwbGllci4nIikKc3Bhcmsuc3FsKGYiQ09NTUVOVCBPTiBUQUJMRSB7Y2F0YWxvZ30uaGVsaW9zX2dvbGQuZGltX2NhdGVnb3J5ICAgIElTICdDYXRlZ29yeSBkaW1lbnNpb24gYW5kIGl0cyBkZXBhcnRtZW50IHJvbGwtdXAgKGZvciBleGFtcGxlIFBST1BVTFNJT04gcm9sbHMgdXAgdG8gUFJPUFVMU0lPTl9BTkRfUE9XRVIpLiciKQpzcGFyay5zcWwoZiJDT01NRU5UIE9OIFRBQkxFIHtjYXRhbG9nfS5oZWxpb3NfZ29sZC5kaW1fc3VwcGxpZXIgICAgSVMgJ1N1cHBsaWVyIGRpbWVuc2lvbiwgb25lIHJvdyBwZXIgc3VwcGxpZXIuJyIpCnNwYXJrLnNxbChmIkNPTU1FTlQgT04gVEFCTEUge2NhdGFsb2d9LmhlbGlvc19nb2xkLmRpbV9jdXN0b21lciAgICBJUyAnQ3VzdG9tZXIgZGltZW5zaW9uIGFzIFNDRCBUeXBlIDIgKHRpZXIgb3ZlciB0aW1lKS4gRmlsdGVyIGlzX2N1cnJlbnQgZm9yIHRoZSBsaXZlIHRpZXI7IG9uZSByb3cgcGVyIGN1c3RvbWVyIHBlciB0aWVyIHZlcnNpb24uJyIpCnNwYXJrLnNxbChmIkNPTU1FTlQgT04gVEFCTEUge2NhdGFsb2d9LmhlbGlvc19nb2xkLmRpbV9kYXRlICAgICAgICBJUyAnR2VuZXJhdGVkIGRhdGUgZGltZW5zaW9uLCBrZXllZCBieSBkYXRlX2tleSAodGhlIHl5eXlNTWRkIGludGVnZXIpLiciKQoKIyBDb2x1bW4gY29tbWVudHM6IGV4cGxhaW4gdGhlIGNvbHVtbnMgdGhlIGJ1c2luZXNzIHNsaWNlcyBhbmQgbWVhc3VyZXMgb24sIGluIGdlbmVyaWMgdGVybXMuCnNwYXJrLnNxbChmIkNPTU1FTlQgT04gQ09MVU1OIHtjYXRhbG9nfS5oZWxpb3NfZ29sZC5mYWN0X29yZGVyX2xpbmVzLmdyb3NzX2Ftb3VudCBJUyAnTGluZSByZXZlbnVlOiBxdWFudGl0eSB0aW1lcyB0aGUgcG9pbnQgaW4gdGltZSB1bml0X3ByaWNlLCBpbiBDUkVESVRTLiciKQpzcGFyay5zcWwoZiJDT01NRU5UIE9OIENPTFVNTiB7Y2F0YWxvZ30uaGVsaW9zX2dvbGQuZmFjdF9vcmRlcl9saW5lcy5tYXJnaW4gICAgICAgSVMgJ0xpbmUgZ3Jvc3MgcHJvZml0OiByZXZlbnVlIG1pbnVzIHRoZSBwb2ludCBpbiB0aW1lIGNvc3QsIGluIENSRURJVFMuIE5lZ2F0aXZlIHdoZW4gdGhlIGxpbmUgc29sZCBiZWxvdyBjb3N0LiciKQpzcGFyay5zcWwoZiJDT01NRU5UIE9OIENPTFVNTiB7Y2F0YWxvZ30uaGVsaW9zX2dvbGQuZmFjdF9vcmRlcl9saW5lcy51bml0X3ByaWNlICAgSVMgJ1ByaWNlIHBlciB1bml0IGluIGZvcmNlIHdoZW4gdGhlIGxpbmUgd2FzIHBsYWNlZCAocG9pbnQgaW4gdGltZSksIGluIENSRURJVFMuJyIpCnNwYXJrLnNxbChmIkNPTU1FTlQgT04gQ09MVU1OIHtjYXRhbG9nfS5oZWxpb3NfZ29sZC5mYWN0X29yZGVyX2xpbmVzLm9yZGVyX3N0YXR1cyBJUyAnQ3VycmVudCBsaWZlY3ljbGUgc3RhdHVzIG9mIHRoZSBvcmRlciB0aGlzIGxpbmUgYmVsb25ncyB0bzogUExBQ0VELCBQQUlELCBQSUNLRUQsIFNISVBQRUQsIERFTElWRVJFRCwgQkFDS09SREVSRUQsIENBTkNFTExFRCBvciBSRVRVUk5FRC4gTlVMTCB3aGVuIHRoZSBvcmRlciBoYXMgbm8gY3VycmVudCByZWNvcmQgKGNoYW5nZSBub3QgeWV0IGFycml2ZWQsIG9yIGEgcHVyZ2VkIGNhbmNlbGxhdGlvbikuIEV4Y2x1ZGUgQ0FOQ0VMTEVEIGZvciBuZXQtb2YtY2FuY2VsbGF0aW9uIHJldmVudWUuJyIpCnNwYXJrLnNxbChmIkNPTU1FTlQgT04gQ09MVU1OIHtjYXRhbG9nfS5oZWxpb3NfZ29sZC5mYWN0X29yZGVyX2xpbmVzLnVuaXRfY29zdCAgICBJUyAnU3VwcGxpZXIgY29zdCBwZXIgdW5pdCBpbiBmb3JjZSB3aGVuIHRoZSBsaW5lIHdhcyBwbGFjZWQgKHBvaW50IGluIHRpbWUpLCBpbiBDUkVESVRTLiciKQpzcGFyay5zcWwoZiJDT01NRU5UIE9OIENPTFVNTiB7Y2F0YWxvZ30uaGVsaW9zX2dvbGQuZmFjdF9vcmRlcl9saW5lcy53YXJlaG91c2VfaWQgSVMgJ1RoZSBkZXBvdCB0aGF0IGZ1bGZpbGxlZCB0aGUgbGluZS4nIikKc3Bhcmsuc3FsKGYiQ09NTUVOVCBPTiBDT0xVTU4ge2NhdGFsb2d9LmhlbGlvc19nb2xkLmZhY3Rfb3JkZXJfbGluZXMucHJvZHVjdF9pZCAgIElTICdUaGUgcHJvZHVjdCBvcmRlcmVkIG9uIHRoZSBsaW5lLiciKQpzcGFyay5zcWwoZiJDT01NRU5UIE9OIENPTFVNTiB7Y2F0YWxvZ30uaGVsaW9zX2dvbGQuZmFjdF9vcmRlcl9saW5lcy5pc19yZXR1cm5lZCAgSVMgJ1RydWUgd2hlbiB0aGUgbGluZSB3YXMgbGF0ZXIgcmV0dXJuZWQuJyIpCnNwYXJrLnNxbChmIkNPTU1FTlQgT04gQ09MVU1OIHtjYXRhbG9nfS5oZWxpb3NfZ29sZC5kaW1fY3VzdG9tZXIudGllciAgICAgICAgICAgICBJUyAnUHJpY2luZyB0aWVyOiBDSEFSVEVSIChiaWcgY29udHJhY3RzKSwgVFJBREUgKHJlZ3VsYXIgYnVzaW5lc3NlcyksIERSSUZURVIgKHdhbGstaW5zKS4gRHJpdmVzIHByaWNpbmcgYW5kIHJvdyBsZXZlbCBzZWN1cml0eS4nIikKc3Bhcmsuc3FsKGYiQ09NTUVOVCBPTiBDT0xVTU4ge2NhdGFsb2d9LmhlbGlvc19nb2xkLmRpbV93YXJlaG91c2UucmVnaW9uICAgICAgICAgIElTICdTb2xhciBzeXN0ZW0gcmVnaW9uOiBJTk5FUiwgQkVMVCBvciBPVVRFUi4nIikKc3Bhcmsuc3FsKGYiQ09NTUVOVCBPTiBDT0xVTU4ge2NhdGFsb2d9LmhlbGlvc19nb2xkLmZhY3RfaW52ZW50b3J5Lm9uX2hhbmQgICAgICAgIElTICdSdW5uaW5nIHN0b2NrIGJhbGFuY2UgYWZ0ZXIgdGhlIG1vdmVtZW50OyB6ZXJvIG1lYW5zIGEgc3RvY2tvdXQuJyIp";
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
# MAGIC ### Self check
# MAGIC Confirm every Gold table and the key business columns now carry a comment.

# COMMAND ----------

semantic = f"{catalog}.helios_semantic"
gold_tables = ["fact_order_lines", "fact_returns", "fact_inventory", "dim_warehouse", "dim_product",
               "dim_category", "dim_supplier", "dim_customer", "dim_date"]

def table_comment(t):
    row = spark.sql(f"SELECT comment FROM {catalog}.information_schema.tables "
                    f"WHERE table_schema = 'helios_gold' AND table_name = '{t}'").first()
    return row and row["comment"]

def column_comment(t, c):
    row = spark.sql(f"SELECT comment FROM {catalog}.information_schema.columns "
                    f"WHERE table_schema = 'helios_gold' AND table_name = '{t}' AND column_name = '{c}'").first()
    return row and row["comment"]

check_true("every Gold table is commented", all(bool(table_comment(t)) for t in gold_tables))
key_columns = [("fact_order_lines", "margin"), ("fact_order_lines", "gross_amount"),
               ("fact_order_lines", "warehouse_id"), ("fact_order_lines", "order_status"), ("dim_customer", "tier"), ("fact_inventory", "on_hand")]
check_true("the key business columns are commented", all(bool(column_comment(t, c)) for t, c in key_columns))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Build the sales combo: order_lines_vw and sales_mv
# MAGIC
# MAGIC Build the first combo: a reporting view `order_lines_vw` at the order-line grain, and the metric view `sales_mv` on it that defines revenue, margin, units, average order value and returns, plus a Net Revenue that nets out cancelled-order lines. This combo answers most of the business questions, and it is where the pricing incident shows up as lines sold below cost.
# MAGIC
# MAGIC **How: the view `order_lines_vw`.** Start from `fact_order_lines` (one row per order line) and LEFT JOIN each dimension so every attribute you want to slice by lands on the same row. Use these joins:
# MAGIC
# MAGIC | Join | Table (alias) | Join condition | Columns it adds |
# MAGIC |---|---|---|---|
# MAGIC | base | `fact_order_lines` (f) | the grain, one row per line | order_line_id, order_id, quantity, unit_price, unit_cost, gross_amount, margin, is_returned, order_status, the foreign keys |
# MAGIC | LEFT JOIN | `dim_product` (p) | `f.product_id = p.product_id` | product_name, sku, hazard_class, category_id |
# MAGIC | LEFT JOIN | `dim_category` (c) | `p.category_id = c.category_id` | category_name, department |
# MAGIC | LEFT JOIN | `dim_supplier` (s) | `f.supplier_id = s.supplier_id` | supplier_name, home_region |
# MAGIC | LEFT JOIN | `dim_warehouse` (w) | `f.warehouse_id = w.warehouse_id` | warehouse_name (the depot), region, body |
# MAGIC | LEFT JOIN | `dim_customer` (cu) | `f.customer_id = cu.customer_id AND cu.is_current` | customer_type, tier |
# MAGIC | LEFT JOIN | `dim_date` (d) | `f.date_key = d.date_key` | date |
# MAGIC | LEFT JOIN | returns per line (r) | `f.order_line_id = r.order_line_id` | returned_quantity, refund_amount |
# MAGIC
# MAGIC - `dim_customer` is SCD Type 2, so a customer has more than one row over time. Add `AND cu.is_current` to the join, so each line matches exactly one (the live) customer row and the join does not multiply rows.
# MAGIC - A line can have a return, and you do not want that to turn one line into several rows, so aggregate returns to one row per line first: `(SELECT order_line_id, SUM(quantity_returned) AS returned_quantity, SUM(refund_amount) AS refund_amount FROM fact_returns GROUP BY order_line_id)`, then LEFT JOIN it and wrap the result in `COALESCE(..., 0)` so a line with no return reads as 0.
# MAGIC - Give the joined columns friendly names, for example `w.warehouse_name AS depot`.
# MAGIC
# MAGIC **How: the metric view `sales_mv`.** Build it with the `source` set to a quoted query over the view, `"SELECT * FROM order_lines_vw"`. Quote the string, so the YAML reader does not misread the `*` (an unquoted `*` is a YAML alias marker). Declare these dimensions (the columns you can group by):
# MAGIC
# MAGIC | Dimension | Expression | What it is |
# MAGIC |---|---|---|
# MAGIC | Depot | depot | the fulfilling depot name |
# MAGIC | Depot Region | depot_region | INNER, BELT or OUTER |
# MAGIC | Category | category_name | the product category |
# MAGIC | Department | department | the category roll-up |
# MAGIC | Product | product_name | the product |
# MAGIC | Product ID | product_id | the product code |
# MAGIC | Supplier | supplier_name | the supplier |
# MAGIC | Supplier ID | supplier_id | the supplier code |
# MAGIC | Customer Type | customer_type | the segment |
# MAGIC | Customer Tier | customer_tier | CHARTER, TRADE or DRIFTER |
# MAGIC | Order Date | order_date | the date the line was placed |
# MAGIC | Sold Below Cost | margin < 0 | true when the line lost money |
# MAGIC | Is Returned | is_returned | whether the line was returned |
# MAGIC | Order Status | order_status | the order's current lifecycle status (NULL if purged/not arrived) |
# MAGIC
# MAGIC And these measures (each written once as an aggregate):
# MAGIC
# MAGIC | Measure | Formula | What it calculates |
# MAGIC |---|---|---|
# MAGIC | Revenue | SUM(gross_amount) | total sales value, in CREDITS |
# MAGIC | Gross Margin | SUM(margin) | total gross profit, in CREDITS |
# MAGIC | Gross Margin Rate | SUM(margin) / SUM(gross_amount) | profit as a share of revenue |
# MAGIC | Units Sold | SUM(quantity) | total units sold |
# MAGIC | Order Lines | COUNT(1) | number of lines |
# MAGIC | Orders | COUNT(DISTINCT order_id) | number of distinct orders |
# MAGIC | Average Order Value | SUM(gross_amount) / COUNT(DISTINCT order_id) | revenue per order |
# MAGIC | Returned Units | SUM(returned_quantity) | units returned |
# MAGIC | Return Rate | SUM(returned_quantity) / SUM(quantity) | returned units over sold units |
# MAGIC | Net Revenue | SUM(CASE WHEN order_status = 'CANCELLED' THEN 0 ELSE gross_amount END) | revenue excluding cancelled-order lines |
# MAGIC | Net Units Sold | SUM(CASE WHEN order_status = 'CANCELLED' THEN 0 ELSE quantity END) | units excluding cancelled-order lines |
# MAGIC
# MAGIC You query a metric view by wrapping a measure in `MEASURE(...)` and adding `GROUP BY ALL`, for example group by the Department dimension and select `MEASURE(Revenue)` to get revenue by department.
# MAGIC
# MAGIC **Example.** Grouping `sales_mv` by Department returns PROPULSION_AND_POWER near 1.34 billion CREDITS at a 0.43 gross margin rate. Filtering to Sold Below Cost returns the incident: about 6,405 lines at a negative gross margin.
# MAGIC
# MAGIC **Definition of done.**
# MAGIC - `order_lines_vw` is one row per order line (its count equals `fact_order_lines`).
# MAGIC - `sales_mv` total `Units Sold` is 1,569,077 and `Revenue` is 1,863,409,489 CREDITS (whole).
# MAGIC - `sales_mv` `Average Order Value` is about 19,255 CREDITS.
# MAGIC - Sliced by `Sold Below Cost` = true, `sales_mv` shows 6,405 order lines and a negative `Gross Margin`.
# MAGIC - `Return Rate` (returned units over sold units) is about 0.0043.
# MAGIC - `Net Revenue` is positive and below gross `Revenue` (cancelled-order lines excluded).
# MAGIC
# MAGIC **References.** You may find these Databricks docs helpful:
# MAGIC - [Unity Catalog metric views](https://docs.databricks.com/aws/en/metric-views/)
# MAGIC - [Metric view YAML syntax reference](https://docs.databricks.com/aws/en/business-semantics/metric-views/yaml-reference)
# MAGIC - [Create and edit metric views](https://docs.databricks.com/aws/en/metric-views/create/sql)
# MAGIC - [measure aggregate function](https://docs.databricks.com/aws/en/sql/language-manual/functions/measure)

# COMMAND ----------

# Your turn. Build order_lines_vw, then sales_mv, then run the self check below.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Task 2: Build the sales combo (solution)
# MAGIC <details>
# MAGIC <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; overflow:auto;"></pre>
# MAGIC
# MAGIC <script>
# MAGIC var codeB64 = "IyBTdGVwIDEuIFRoZSByZXBvcnRpbmcgdmlldzogb25lIGZsYXQsIGJ1c2luZXNzLW5hbWVkIHJvdyBwZXIgb3JkZXIgbGluZS4gSXQgam9pbnMgdGhlIGZhY3QgdG8gZWFjaCBvZiBpdHMKIyBkaW1lbnNpb25zIHNvIGV2ZXJ5IGF0dHJpYnV0ZSB5b3Ugc2xpY2UgYnkgc2l0cyBvbiBvbmUgcm93LiBSZXR1cm5zIGFyZSBhZ2dyZWdhdGVkIHBlciBsaW5lIGZpcnN0LCBzbyBhIGxpbmUKIyB0aGF0IGhhcyBhIHJldHVybiBzdGlsbCBzdGF5cyBleGFjdGx5IG9uZSByb3cgKHRoZSBqb2luIGNhbm5vdCBmYW4gb3V0KS4Kc3Bhcmsuc3FsKGYiIiIKQ1JFQVRFIE9SIFJFUExBQ0UgVklFVyB7Y2F0YWxvZ30uaGVsaW9zX3NlbWFudGljLm9yZGVyX2xpbmVzX3Z3IEFTClNFTEVDVAogIGYub3JkZXJfbGluZV9pZCwgZi5vcmRlcl9pZCwgZi5wcm9kdWN0X2lkLCBmLmN1c3RvbWVyX2lkLCBmLndhcmVob3VzZV9pZCwgZi5zdXBwbGllcl9pZCwKICBmLmRhdGVfa2V5LCBmLnByaWNlX3NrLCBmLnF1YW50aXR5LCBmLnVuaXRfcHJpY2UsIGYudW5pdF9jb3N0LCBmLmdyb3NzX2Ftb3VudCwgZi5tYXJnaW4sIGYuaXNfcmV0dXJuZWQsIGYub3JkZXJfc3RhdHVzLAogIHcud2FyZWhvdXNlX25hbWUgQVMgZGVwb3QsIHcucmVnaW9uIEFTIGRlcG90X3JlZ2lvbiwgdy5ib2R5IEFTIGRlcG90X2JvZHksCiAgYy5jYXRlZ29yeV9uYW1lLCBjLmRlcGFydG1lbnQsCiAgcC5wcm9kdWN0X25hbWUsIHAuc2t1IEFTIHByb2R1Y3Rfc2t1LCBwLmhhemFyZF9jbGFzcywKICBzLnN1cHBsaWVyX25hbWUsIHMuaG9tZV9yZWdpb24gQVMgc3VwcGxpZXJfcmVnaW9uLAogIGN1LmN1c3RvbWVyX3R5cGUsIGN1LnRpZXIgQVMgY3VzdG9tZXJfdGllciwKICBkLmRhdGUgQVMgb3JkZXJfZGF0ZSwKICBDT0FMRVNDRShyLnJldHVybmVkX3F1YW50aXR5LCAwKSBBUyByZXR1cm5lZF9xdWFudGl0eSwKICBDT0FMRVNDRShyLnJlZnVuZF9hbW91bnQsIDApICAgICBBUyByZWZ1bmRfYW1vdW50CkZST00ge2NhdGFsb2d9LmhlbGlvc19nb2xkLmZhY3Rfb3JkZXJfbGluZXMgZgpMRUZUIEpPSU4ge2NhdGFsb2d9LmhlbGlvc19nb2xkLmRpbV9wcm9kdWN0ICAgcCAgT04gZi5wcm9kdWN0X2lkICAgPSBwLnByb2R1Y3RfaWQKTEVGVCBKT0lOIHtjYXRhbG9nfS5oZWxpb3NfZ29sZC5kaW1fY2F0ZWdvcnkgIGMgIE9OIHAuY2F0ZWdvcnlfaWQgID0gYy5jYXRlZ29yeV9pZApMRUZUIEpPSU4ge2NhdGFsb2d9LmhlbGlvc19nb2xkLmRpbV9zdXBwbGllciAgcyAgT04gZi5zdXBwbGllcl9pZCAgPSBzLnN1cHBsaWVyX2lkCkxFRlQgSk9JTiB7Y2F0YWxvZ30uaGVsaW9zX2dvbGQuZGltX3dhcmVob3VzZSB3ICBPTiBmLndhcmVob3VzZV9pZCA9IHcud2FyZWhvdXNlX2lkCkxFRlQgSk9JTiB7Y2F0YWxvZ30uaGVsaW9zX2dvbGQuZGltX2N1c3RvbWVyICBjdSBPTiBmLmN1c3RvbWVyX2lkICA9IGN1LmN1c3RvbWVyX2lkIEFORCBjdS5pc19jdXJyZW50CkxFRlQgSk9JTiB7Y2F0YWxvZ30uaGVsaW9zX2dvbGQuZGltX2RhdGUgICAgICBkICBPTiBmLmRhdGVfa2V5ICAgICA9IGQuZGF0ZV9rZXkKTEVGVCBKT0lOICgKICBTRUxFQ1Qgb3JkZXJfbGluZV9pZCwgU1VNKHF1YW50aXR5X3JldHVybmVkKSBBUyByZXR1cm5lZF9xdWFudGl0eSwgU1VNKHJlZnVuZF9hbW91bnQpIEFTIHJlZnVuZF9hbW91bnQKICBGUk9NIHtjYXRhbG9nfS5oZWxpb3NfZ29sZC5mYWN0X3JldHVybnMgR1JPVVAgQlkgb3JkZXJfbGluZV9pZAopIHIgT04gZi5vcmRlcl9saW5lX2lkID0gci5vcmRlcl9saW5lX2lkCiIiIikKCiMgU3RlcCAyLiBUaGUgbWV0cmljIHZpZXcgc2l0cyBvbiB0aGUgcmVwb3J0aW5nIHZpZXcuIFRoZSBzb3VyY2UgaXMgYSBTRUxFQ1QgcXVlcnkgb3ZlciB0aGUgdmlldzsgdGhlCiMgZGltZW5zaW9ucyBhcmUgdGhlIGNvbHVtbnMgeW91IHNsaWNlIGJ5OyB0aGUgbWVhc3VyZXMgYXJlIGFnZ3JlZ2F0ZXMgdGhlIGVuZ2luZSBjb21wdXRlcyBhdCBxdWVyeSB0aW1lLgojIFF1ZXJ5IGEgbWV0cmljIHZpZXcgd2l0aCBNRUFTVVJFKDxtZWFzdXJlIG5hbWU+KSBhbmQgR1JPVVAgQlkgQUxMLgpzcGFyay5zcWwoZiIiIgpDUkVBVEUgT1IgUkVQTEFDRSBWSUVXIHtjYXRhbG9nfS5oZWxpb3Nfc2VtYW50aWMuc2FsZXNfbXYKV0lUSCBNRVRSSUNTCkxBTkdVQUdFIFlBTUwKQVMgJCQKdmVyc2lvbjogMS4xCnNvdXJjZTogIlNFTEVDVCAqIEZST00ge2NhdGFsb2d9LmhlbGlvc19zZW1hbnRpYy5vcmRlcl9saW5lc192dyIKY29tbWVudDogIkhlbGlvcyBzYWxlcyBhdCB0aGUgb3JkZXItbGluZSBncmFpbjogcmV2ZW51ZSwgZ3Jvc3MgbWFyZ2luLCB1bml0cywgb3JkZXJzLCBhdmVyYWdlIG9yZGVyIHZhbHVlIGFuZCByZXR1cm5zLCBhbGwgcG9pbnQgaW4gdGltZSB2YWx1ZWQuIFNsaWNlIGJ5IGRlcG90LCByZWdpb24sIGNhdGVnb3J5LCBkZXBhcnRtZW50LCBwcm9kdWN0LCBzdXBwbGllciwgY3VzdG9tZXIgdHlwZSBhbmQgdGllciwgYW5kIG9yZGVyIGRhdGUuIFNvbGQgQmVsb3cgQ29zdCBpc29sYXRlcyBsb3NzLW1ha2luZyBsaW5lcy4gTmV0IFJldmVudWUgYW5kIE5ldCBVbml0cyBTb2xkIGV4Y2x1ZGUgY2FuY2VsbGVkLW9yZGVyIGxpbmVzIChvcmRlcl9zdGF0dXMgPSBDQU5DRUxMRUQpOyBPcmRlciBTdGF0dXMgc2xpY2VzIGJ5IGxpZmVjeWNsZSBzdGF0ZS4iCmRpbWVuc2lvbnM6CiAgLSBuYW1lOiBEZXBvdAogICAgZXhwcjogZGVwb3QKICAgIGNvbW1lbnQ6ICJGdWxmaWxsaW5nIGRlcG90IG5hbWUiCiAgLSBuYW1lOiBEZXBvdCBSZWdpb24KICAgIGV4cHI6IGRlcG90X3JlZ2lvbgogICAgY29tbWVudDogIklOTkVSLCBCRUxUIG9yIE9VVEVSIgogIC0gbmFtZTogQ2F0ZWdvcnkKICAgIGV4cHI6IGNhdGVnb3J5X25hbWUKICAgIGNvbW1lbnQ6ICJQcm9kdWN0IGNhdGVnb3J5IgogIC0gbmFtZTogRGVwYXJ0bWVudAogICAgZXhwcjogZGVwYXJ0bWVudAogICAgY29tbWVudDogIkNhdGVnb3J5IHJvbGwtdXAgZm9yIGRhc2hib2FyZHMiCiAgLSBuYW1lOiBQcm9kdWN0CiAgICBleHByOiBwcm9kdWN0X25hbWUKICAtIG5hbWU6IFByb2R1Y3QgSUQKICAgIGV4cHI6IHByb2R1Y3RfaWQKICAgIGNvbW1lbnQ6ICJQcm9kdWN0IGNvZGUiCiAgLSBuYW1lOiBTdXBwbGllcgogICAgZXhwcjogc3VwcGxpZXJfbmFtZQogIC0gbmFtZTogU3VwcGxpZXIgSUQKICAgIGV4cHI6IHN1cHBsaWVyX2lkCiAgICBjb21tZW50OiAiU3VwcGxpZXIgY29kZSIKICAtIG5hbWU6IEN1c3RvbWVyIFR5cGUKICAgIGV4cHI6IGN1c3RvbWVyX3R5cGUKICAgIGNvbW1lbnQ6ICJTZWdtZW50OiBGUkVJR0hUX0ZMRUVULCBNSU5FUiwgTElORVIsIFJFU0VBUkNILCBQQVRST0wsIElOREVQRU5ERU5ULCBDT0xPTklBTCIKICAtIG5hbWU6IEN1c3RvbWVyIFRpZXIKICAgIGV4cHI6IGN1c3RvbWVyX3RpZXIKICAgIGNvbW1lbnQ6ICJDdXJyZW50IHByaWNpbmcgdGllcjogQ0hBUlRFUiwgVFJBREUgb3IgRFJJRlRFUiIKICAtIG5hbWU6IE9yZGVyIERhdGUKICAgIGV4cHI6IG9yZGVyX2RhdGUKICAgIGNvbW1lbnQ6ICJEYXRlIHRoZSBsaW5lIHdhcyBwbGFjZWQiCiAgLSBuYW1lOiBTb2xkIEJlbG93IENvc3QKICAgIGV4cHI6IG1hcmdpbiA8IDAKICAgIGNvbW1lbnQ6ICJUcnVlIHdoZW4gdGhlIGxpbmUgc29sZCB1bmRlciBjb3N0IChuZWdhdGl2ZSBtYXJnaW4pIgogIC0gbmFtZTogSXMgUmV0dXJuZWQKICAgIGV4cHI6IGlzX3JldHVybmVkCiAgLSBuYW1lOiBPcmRlciBTdGF0dXMKICAgIGV4cHI6IG9yZGVyX3N0YXR1cwogICAgY29tbWVudDogIkN1cnJlbnQgbGlmZWN5Y2xlIHN0YXR1cyBvZiB0aGUgbGluZSdzIG9yZGVyOyBOVUxMIGlmIHB1cmdlZCBvciBub3QgeWV0IGFycml2ZWQiCm1lYXN1cmVzOgogIC0gbmFtZTogUmV2ZW51ZQogICAgZXhwcjogU1VNKGdyb3NzX2Ftb3VudCkKICAgIGNvbW1lbnQ6ICJTdW0gb2YgZ3Jvc3NfYW1vdW50IChxdWFudGl0eSB0aW1lcyBwb2ludCBpbiB0aW1lIHVuaXRfcHJpY2UpLCBpbiBDUkVESVRTIgogIC0gbmFtZTogR3Jvc3MgTWFyZ2luCiAgICBleHByOiBTVU0obWFyZ2luKQogICAgY29tbWVudDogIlJldmVudWUgbWludXMgcG9pbnQgaW4gdGltZSBjb3N0LCBpbiBDUkVESVRTIgogIC0gbmFtZTogR3Jvc3MgTWFyZ2luIFJhdGUKICAgIGV4cHI6IFNVTShtYXJnaW4pIC8gU1VNKGdyb3NzX2Ftb3VudCkKICAgIGNvbW1lbnQ6ICJHcm9zcyBNYXJnaW4gZGl2aWRlZCBieSBSZXZlbnVlIgogIC0gbmFtZTogVW5pdHMgU29sZAogICAgZXhwcjogU1VNKHF1YW50aXR5KQogIC0gbmFtZTogT3JkZXIgTGluZXMKICAgIGV4cHI6IENPVU5UKDEpCiAgLSBuYW1lOiBPcmRlcnMKICAgIGV4cHI6IENPVU5UKERJU1RJTkNUIG9yZGVyX2lkKQogIC0gbmFtZTogQXZlcmFnZSBPcmRlciBWYWx1ZQogICAgZXhwcjogU1VNKGdyb3NzX2Ftb3VudCkgLyBDT1VOVChESVNUSU5DVCBvcmRlcl9pZCkKICAgIGNvbW1lbnQ6ICJSZXZlbnVlIGRpdmlkZWQgYnkgZGlzdGluY3Qgb3JkZXJzLCBpbiBDUkVESVRTIgogIC0gbmFtZTogQXZlcmFnZSBVbml0IFByaWNlCiAgICBleHByOiBTVU0oZ3Jvc3NfYW1vdW50KSAvIFNVTShxdWFudGl0eSkKICAtIG5hbWU6IFJldHVybmVkIFVuaXRzCiAgICBleHByOiBTVU0ocmV0dXJuZWRfcXVhbnRpdHkpCiAgLSBuYW1lOiBSZWZ1bmQgQW1vdW50CiAgICBleHByOiBTVU0ocmVmdW5kX2Ftb3VudCkKICAgIGNvbW1lbnQ6ICJUb3RhbCByZWZ1bmRlZCwgdmFsdWVkIGF0IHRoZSBvcmlnaW5hbCBwYWlkIHByaWNlLCBpbiBDUkVESVRTIgogIC0gbmFtZTogUmV0dXJuIFJhdGUKICAgIGV4cHI6IFNVTShyZXR1cm5lZF9xdWFudGl0eSkgLyBTVU0ocXVhbnRpdHkpCiAgICBjb21tZW50OiAiUmV0dXJuZWQgdW5pdHMgZGl2aWRlZCBieSBzb2xkIHVuaXRzIgogIC0gbmFtZTogTmV0IFJldmVudWUKICAgIGV4cHI6IFNVTShDQVNFIFdIRU4gb3JkZXJfc3RhdHVzID0gJ0NBTkNFTExFRCcgVEhFTiAwIEVMU0UgZ3Jvc3NfYW1vdW50IEVORCkKICAgIGNvbW1lbnQ6ICJSZXZlbnVlIGV4Y2x1ZGluZyBjYW5jZWxsZWQtb3JkZXIgbGluZXMgKGdyb3NzIGJvb2tpbmdzIG1pbnVzIHZpc2libGUgY2FuY2VsbGF0aW9ucyksIGluIENSRURJVFMiCiAgLSBuYW1lOiBOZXQgVW5pdHMgU29sZAogICAgZXhwcjogU1VNKENBU0UgV0hFTiBvcmRlcl9zdGF0dXMgPSAnQ0FOQ0VMTEVEJyBUSEVOIDAgRUxTRSBxdWFudGl0eSBFTkQpCiAgICBjb21tZW50OiAiVW5pdHMgZXhjbHVkaW5nIGNhbmNlbGxlZC1vcmRlciBsaW5lcyIKJCQKIiIiKQ==";
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
# MAGIC ### Self check
# MAGIC Reconcile the view to the fact, then read `sales_mv` with `MEASURE()`.

# COMMAND ----------

semantic = f"{catalog}.helios_semantic"
view = spark.table(f"{semantic}.order_lines_vw")
fact = spark.table(f"{catalog}.helios_gold.fact_order_lines")
check("order_lines_vw is one row per order line", fact.count(), view.count())

units = spark.sql(f"SELECT MEASURE(`Units Sold`) v FROM {semantic}.sales_mv").first()["v"]
check("sales_mv total units sold", 1569077, units)
revenue = spark.sql(f"SELECT CAST(MEASURE(`Revenue`) AS BIGINT) v FROM {semantic}.sales_mv").first()["v"]
check("sales_mv total revenue (whole CREDITS)", 1863409489, revenue)
net_revenue = spark.sql(f"SELECT CAST(MEASURE(`Net Revenue`) AS BIGINT) v FROM {semantic}.sales_mv").first()["v"]
check_true("net revenue is positive and below gross revenue (cancelled lines excluded)", 0 < net_revenue < revenue)
aov = spark.sql(f"SELECT CAST(MEASURE(`Average Order Value`) AS BIGINT) v FROM {semantic}.sales_mv").first()["v"]
check("sales_mv average order value (whole CREDITS)", 19255, aov)

below = spark.sql(f"SELECT MEASURE(`Order Lines`) lines, CAST(MEASURE(`Gross Margin`) AS BIGINT) margin "
                  f"FROM {semantic}.sales_mv WHERE `Sold Below Cost` GROUP BY ALL").first()
check("order lines sold below cost (the incident)", 6405, below["lines"])
check_true("below-cost gross margin is negative", below["margin"] < 0)

rate = spark.sql(f"SELECT ROUND(MEASURE(`Return Rate`), 4) v FROM {semantic}.sales_mv").first()["v"]
check("return rate (returned units / sold units)", "0.0043", str(rate))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Build the orders combo: orders_vw and orders_mv
# MAGIC
# MAGIC Build the order-lifecycle combo: a view `orders_vw` at one row per order, and the metric view `orders_mv` that defines the cancellation rate and the on-time fulfilment rate. The catch is on-time fulfilment: the star keeps each order's current status but not its history, so you rebuild one extra flag from the raw change feed.
# MAGIC
# MAGIC **How: the view `orders_vw`.** Start from `silver_orders` (one row per order, current state) and add the backordered flag and the slicing attributes:
# MAGIC
# MAGIC | Join | Table (alias) | Join condition | Columns it adds |
# MAGIC |---|---|---|---|
# MAGIC | base | `silver_orders` (o) | one row per order, current state | order_id, customer_id, warehouse_id, channel, order_ts, status |
# MAGIC | LEFT JOIN | backordered flag (b) | `o.order_id = b.order_id` | ever_backordered |
# MAGIC | LEFT JOIN | `dim_warehouse` (w) | `o.warehouse_id = w.warehouse_id` | warehouse_name (depot), region |
# MAGIC | LEFT JOIN | `dim_customer` (cu) | `o.customer_id = cu.customer_id AND cu.is_current` | customer_type, tier |
# MAGIC
# MAGIC - `silver_orders` carries only the order's current status, so it cannot tell you whether an order was ever BACKORDERED earlier. The raw change feed `bronze_orders` carries every status the order ever had, so build the flag there: `(SELECT order_id, MAX(status = 'BACKORDERED') AS ever_backordered FROM bronze_orders GROUP BY order_id)`, then LEFT JOIN it on `order_id`. `MAX(condition)` is true if the condition was ever true.
# MAGIC - Wrap the flag in `COALESCE(..., false)`, derive `order_date` with `to_date(order_ts)`, and join `dim_customer` on `is_current` as in Task 2.
# MAGIC
# MAGIC **How: the metric view `orders_mv`.** Build it with the `source` set to the quoted query `"SELECT * FROM orders_vw"` (quote it, as in Task 2, so YAML does not misread the `*`). Dimensions:
# MAGIC
# MAGIC | Dimension | Expression | What it is |
# MAGIC |---|---|---|
# MAGIC | Depot | depot | the fulfilling depot |
# MAGIC | Depot Region | depot_region | INNER, BELT or OUTER |
# MAGIC | Channel | channel | API, CONSOLE or COUNTER |
# MAGIC | Order Status | status | the current status |
# MAGIC | Customer Type | customer_type | the segment |
# MAGIC | Customer Tier | customer_tier | CHARTER, TRADE or DRIFTER |
# MAGIC | Ever Backordered | ever_backordered | true if it ever hit BACKORDERED |
# MAGIC | Order Date | order_date | the date the order was placed |
# MAGIC
# MAGIC Measures. A `FILTER (WHERE ...)` measure counts only the rows that match the condition, so it is a conditional count; a rate divides one conditional count by the total:
# MAGIC
# MAGIC | Measure | Formula | What it calculates |
# MAGIC |---|---|---|
# MAGIC | Orders | COUNT(1) | number of orders |
# MAGIC | Cancelled Orders | COUNT(1) FILTER (WHERE status = 'CANCELLED') | orders currently cancelled |
# MAGIC | Cancellation Rate | COUNT(1) FILTER (WHERE status = 'CANCELLED') / COUNT(1) | cancelled over all orders |
# MAGIC | Delivered Orders | COUNT(1) FILTER (WHERE status = 'DELIVERED') | orders delivered |
# MAGIC | On Time Fulfilments | COUNT(1) FILTER (WHERE status = 'DELIVERED' AND NOT ever_backordered) | delivered and never backordered |
# MAGIC | On Time Fulfilment Rate | (On Time Fulfilments) / COUNT(1) | the share delivered without a backorder |
# MAGIC | Backordered Orders | COUNT(1) FILTER (WHERE ever_backordered) | orders that hit a backorder |
# MAGIC
# MAGIC **Example.** `orders_mv` total `Cancellation Rate` is about 0.041 and `On Time Fulfilment Rate` about 0.674. Sliced by depot on 2257-03-03, Ares Depot has the highest cancellation rate (near 0.097, more than triple the calm depots).
# MAGIC
# MAGIC **Definition of done.**
# MAGIC - `orders_mv` `Cancellation Rate` is about 0.0408 and `On Time Fulfilment Rate` about 0.6737.
# MAGIC - Sliced by depot on the incident day 2257-03-03, the highest cancellation rate is Ares Depot.
# MAGIC
# MAGIC **References.** You may find these Databricks docs helpful:
# MAGIC - [Metric view YAML syntax reference](https://docs.databricks.com/aws/en/business-semantics/metric-views/yaml-reference)
# MAGIC - [Create and edit metric views](https://docs.databricks.com/aws/en/metric-views/create/sql)
# MAGIC - [measure aggregate function](https://docs.databricks.com/aws/en/sql/language-manual/functions/measure)

# COMMAND ----------

# Your turn. Build orders_vw, then orders_mv, then run the self check below.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Task 3: Build the orders combo (solution)
# MAGIC <details>
# MAGIC <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; overflow:auto;"></pre>
# MAGIC
# MAGIC <script>
# MAGIC var codeB64 = "IyBTdGVwIDEuIFRoZSByZXBvcnRpbmcgdmlldzogb25lIHJvdyBwZXIgb3JkZXIgKGl0cyBjdXJyZW50IHN0YXRlKSwgcGx1cyB3aGV0aGVyIGl0IHdhcyBldmVyIEJBQ0tPUkRFUkVELgojIHNpbHZlcl9vcmRlcnMga2VlcHMgb25seSB0aGUgY3VycmVudCBzdGF0dXMsIHNvIHRoZSBldmVyX2JhY2tvcmRlcmVkIGZsYWcgaXMgcmVidWlsdCBmcm9tIHRoZSByYXcgY2hhbmdlCiMgZmVlZCBpbiBicm9uemUgKE1BWCBvdmVyIHRoZSBvcmRlcidzIGhpc3RvcnkpLiBEZXBvdCBhbmQgY3VycmVudCBjdXN0b21lciBhdHRyaWJ1dGVzIGFyZSBqb2luZWQgb24gZm9yIHNsaWNpbmcuCnNwYXJrLnNxbChmIiIiCkNSRUFURSBPUiBSRVBMQUNFIFZJRVcge2NhdGFsb2d9LmhlbGlvc19zZW1hbnRpYy5vcmRlcnNfdncgQVMKU0VMRUNUCiAgby5vcmRlcl9pZCwgby5jdXN0b21lcl9pZCwgby53YXJlaG91c2VfaWQsIG8uY2hhbm5lbCwgby5vcmRlcl90cywgby5zdGF0dXMsCiAgQ09BTEVTQ0UoYi5ldmVyX2JhY2tvcmRlcmVkLCBmYWxzZSkgQVMgZXZlcl9iYWNrb3JkZXJlZCwKICB0b19kYXRlKG8ub3JkZXJfdHMpIEFTIG9yZGVyX2RhdGUsCiAgdy53YXJlaG91c2VfbmFtZSBBUyBkZXBvdCwgdy5yZWdpb24gQVMgZGVwb3RfcmVnaW9uLAogIGN1LmN1c3RvbWVyX3R5cGUsIGN1LnRpZXIgQVMgY3VzdG9tZXJfdGllcgpGUk9NIHtjYXRhbG9nfS5oZWxpb3Nfc2lsdmVyLnNpbHZlcl9vcmRlcnMgbwpMRUZUIEpPSU4gKAogIFNFTEVDVCBvcmRlcl9pZCwgTUFYKHN0YXR1cyA9ICdCQUNLT1JERVJFRCcpIEFTIGV2ZXJfYmFja29yZGVyZWQKICBGUk9NIHtjYXRhbG9nfS5oZWxpb3NfYnJvbnplLmJyb256ZV9vcmRlcnMgR1JPVVAgQlkgb3JkZXJfaWQKKSBiIE9OIG8ub3JkZXJfaWQgPSBiLm9yZGVyX2lkCkxFRlQgSk9JTiB7Y2F0YWxvZ30uaGVsaW9zX2dvbGQuZGltX3dhcmVob3VzZSB3ICBPTiBvLndhcmVob3VzZV9pZCA9IHcud2FyZWhvdXNlX2lkCkxFRlQgSk9JTiB7Y2F0YWxvZ30uaGVsaW9zX2dvbGQuZGltX2N1c3RvbWVyICBjdSBPTiBvLmN1c3RvbWVyX2lkICA9IGN1LmN1c3RvbWVyX2lkIEFORCBjdS5pc19jdXJyZW50CiIiIikKCiMgU3RlcCAyLiBUaGUgbWV0cmljIHZpZXcuIFRoZSByYXRlIG1lYXN1cmVzIHVzZSBGSUxURVIgKFdIRVJFIC4uLiksIHdoaWNoIGNvdW50cyBvbmx5IHRoZSByb3dzIHRoYXQgbWF0Y2ggdGhlCiMgY29uZGl0aW9uLCBzbyBDYW5jZWxsYXRpb24gUmF0ZSBpcyBjYW5jZWxsZWQgb3JkZXJzIG92ZXIgYWxsIG9yZGVycyBhbmQgT24gVGltZSBGdWxmaWxtZW50IFJhdGUgaXMgdGhlIHNoYXJlCiMgdGhhdCByZWFjaGVkIERFTElWRVJFRCB3aXRob3V0IGV2ZXIgYmVpbmcgQkFDS09SREVSRUQuCnNwYXJrLnNxbChmIiIiCkNSRUFURSBPUiBSRVBMQUNFIFZJRVcge2NhdGFsb2d9LmhlbGlvc19zZW1hbnRpYy5vcmRlcnNfbXYKV0lUSCBNRVRSSUNTCkxBTkdVQUdFIFlBTUwKQVMgJCQKdmVyc2lvbjogMS4xCnNvdXJjZTogIlNFTEVDVCAqIEZST00ge2NhdGFsb2d9LmhlbGlvc19zZW1hbnRpYy5vcmRlcnNfdnciCmNvbW1lbnQ6ICJIZWxpb3Mgb3JkZXIgbGlmZWN5Y2xlOiBvcmRlciB2b2x1bWUsIGNhbmNlbGxhdGlvbiByYXRlIGFuZCBvbi10aW1lIGZ1bGZpbG1lbnQuIEdyYWluIGlzIG9uZSBvcmRlciAoY3VycmVudCBzdGF0ZSkuIE9uLXRpbWUgbWVhbnMgdGhlIG9yZGVyIHJlYWNoZWQgREVMSVZFUkVEIHdpdGhvdXQgZXZlciBiZWluZyBCQUNLT1JERVJFRC4gU2xpY2UgYnkgZGVwb3QsIGNoYW5uZWwsIHN0YXR1cywgY3VzdG9tZXIgdHlwZSBhbmQgdGllciwgYW5kIG9yZGVyIGRhdGUuIgpkaW1lbnNpb25zOgogIC0gbmFtZTogRGVwb3QKICAgIGV4cHI6IGRlcG90CiAgLSBuYW1lOiBEZXBvdCBSZWdpb24KICAgIGV4cHI6IGRlcG90X3JlZ2lvbgogIC0gbmFtZTogQ2hhbm5lbAogICAgZXhwcjogY2hhbm5lbAogICAgY29tbWVudDogIkhvdyB0aGUgb3JkZXIgd2FzIHBsYWNlZDogQVBJLCBDT05TT0xFIG9yIENPVU5URVIiCiAgLSBuYW1lOiBPcmRlciBTdGF0dXMKICAgIGV4cHI6IHN0YXR1cwogICAgY29tbWVudDogIkN1cnJlbnQgc3RhdHVzOiBQTEFDRUQsIFBBSUQsIFBJQ0tFRCwgU0hJUFBFRCwgREVMSVZFUkVELCBCQUNLT1JERVJFRCwgQ0FOQ0VMTEVELCBSRVRVUk5FRCIKICAtIG5hbWU6IEN1c3RvbWVyIFR5cGUKICAgIGV4cHI6IGN1c3RvbWVyX3R5cGUKICAtIG5hbWU6IEN1c3RvbWVyIFRpZXIKICAgIGV4cHI6IGN1c3RvbWVyX3RpZXIKICAtIG5hbWU6IEV2ZXIgQmFja29yZGVyZWQKICAgIGV4cHI6IGV2ZXJfYmFja29yZGVyZWQKICAgIGNvbW1lbnQ6ICJUcnVlIGlmIHRoZSBvcmRlciBwYXNzZWQgdGhyb3VnaCBCQUNLT1JERVJFRCBhdCBhbnkgcG9pbnQgaW4gaXRzIGhpc3RvcnkiCiAgLSBuYW1lOiBPcmRlciBEYXRlCiAgICBleHByOiBvcmRlcl9kYXRlCm1lYXN1cmVzOgogIC0gbmFtZTogT3JkZXJzCiAgICBleHByOiBDT1VOVCgxKQogIC0gbmFtZTogQ2FuY2VsbGVkIE9yZGVycwogICAgZXhwcjogQ09VTlQoMSkgRklMVEVSIChXSEVSRSBzdGF0dXMgPSAnQ0FOQ0VMTEVEJykKICAtIG5hbWU6IENhbmNlbGxhdGlvbiBSYXRlCiAgICBleHByOiBDT1VOVCgxKSBGSUxURVIgKFdIRVJFIHN0YXR1cyA9ICdDQU5DRUxMRUQnKSAvIENPVU5UKDEpCiAgICBjb21tZW50OiAiQ2FuY2VsbGVkIG9yZGVycyBkaXZpZGVkIGJ5IGFsbCBvcmRlcnMiCiAgLSBuYW1lOiBEZWxpdmVyZWQgT3JkZXJzCiAgICBleHByOiBDT1VOVCgxKSBGSUxURVIgKFdIRVJFIHN0YXR1cyA9ICdERUxJVkVSRUQnKQogIC0gbmFtZTogT24gVGltZSBGdWxmaWxtZW50cwogICAgZXhwcjogQ09VTlQoMSkgRklMVEVSIChXSEVSRSBzdGF0dXMgPSAnREVMSVZFUkVEJyBBTkQgTk9UIGV2ZXJfYmFja29yZGVyZWQpCiAgLSBuYW1lOiBPbiBUaW1lIEZ1bGZpbG1lbnQgUmF0ZQogICAgZXhwcjogQ09VTlQoMSkgRklMVEVSIChXSEVSRSBzdGF0dXMgPSAnREVMSVZFUkVEJyBBTkQgTk9UIGV2ZXJfYmFja29yZGVyZWQpIC8gQ09VTlQoMSkKICAgIGNvbW1lbnQ6ICJTaGFyZSBvZiBvcmRlcnMgdGhhdCByZWFjaGVkIERFTElWRVJFRCB3aXRob3V0IGEgQkFDS09SREVSRUQgc3RlcCIKICAtIG5hbWU6IEJhY2tvcmRlcmVkIE9yZGVycwogICAgZXhwcjogQ09VTlQoMSkgRklMVEVSIChXSEVSRSBldmVyX2JhY2tvcmRlcmVkKQokJAoiIiIp";
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
# MAGIC ### Self check
# MAGIC Read `orders_mv` with `MEASURE()` and confirm the lifecycle rates and the incident-day signal.

# COMMAND ----------

semantic = f"{catalog}.helios_semantic"

cancel = spark.sql(f"SELECT ROUND(MEASURE(`Cancellation Rate`), 4) v FROM {semantic}.orders_mv").first()["v"]
check("orders_mv cancellation rate", "0.0408", str(cancel))
ontime = spark.sql(f"SELECT ROUND(MEASURE(`On Time Fulfilment Rate`), 4) v FROM {semantic}.orders_mv").first()["v"]
check("orders_mv on-time fulfilment rate", "0.6737", str(ontime))

worst = spark.sql(f"SELECT `Depot` d FROM {semantic}.orders_mv WHERE `Order Date` = DATE'2257-03-03' "
                  f"GROUP BY ALL ORDER BY MEASURE(`Cancellation Rate`) DESC LIMIT 1").first()["d"]
check("worst depot for cancellations on the incident day", "Ares Depot", worst)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Build the inventory combo: inventory_vw and inventory_mv
# MAGIC
# MAGIC Build the stock-health combo: a view `inventory_vw` at one row per stock movement, and the metric view `inventory_mv` that measures units in and out and the stockouts. This is the simplest combo, straight off the movement fact, and it is where the Ares stockout shows as `Min On Hand` reaching 0.
# MAGIC
# MAGIC **How: the view `inventory_vw`.** Start from `fact_inventory` (one row per stock movement) and join the depot and product attributes:
# MAGIC
# MAGIC | Join | Table (alias) | Join condition | Columns it adds |
# MAGIC |---|---|---|---|
# MAGIC | base | `fact_inventory` (i) | one row per stock movement | warehouse_id, product_id, movement_ts, date_key, delta, on_hand |
# MAGIC | LEFT JOIN | `dim_warehouse` (w) | `i.warehouse_id = w.warehouse_id` | warehouse_name (depot), region |
# MAGIC | LEFT JOIN | `dim_product` (p) | `i.product_id = p.product_id` | product_name, category_id |
# MAGIC | LEFT JOIN | `dim_category` (c) | `p.category_id = c.category_id` | category_name, department |
# MAGIC | LEFT JOIN | `dim_date` (d) | `i.date_key = d.date_key` | date |
# MAGIC
# MAGIC - `delta` is the signed change (negative on a sale, positive on a restock), and `on_hand` is the running balance after the movement. Carry both through.
# MAGIC - Name the joined date column `movement_date`.
# MAGIC
# MAGIC **How: the metric view `inventory_mv`.** Build it with the `source` set to the quoted query `"SELECT * FROM inventory_vw"` (quote it, as in Task 2, so YAML does not misread the `*`). Dimensions:
# MAGIC
# MAGIC | Dimension | Expression | What it is |
# MAGIC |---|---|---|
# MAGIC | Depot | depot | the depot |
# MAGIC | Depot Region | depot_region | INNER, BELT or OUTER |
# MAGIC | Product | product_name | the product |
# MAGIC | Product ID | product_id | the product code |
# MAGIC | Category | category_name | the category |
# MAGIC | Department | department | the category roll-up |
# MAGIC | Movement Date | movement_date | the date of the movement |
# MAGIC | Is Stockout | on_hand = 0 | true when stock hit zero |
# MAGIC
# MAGIC Measures. `Units Out` and `Units In` split the signed `delta` with a CASE; `Stockout Events` counts the movements that hit zero; `Min On Hand` is the lowest balance reached:
# MAGIC
# MAGIC | Measure | Formula | What it calculates |
# MAGIC |---|---|---|
# MAGIC | Movements | COUNT(1) | number of stock movements |
# MAGIC | Stockout Events | COUNT(1) FILTER (WHERE on_hand = 0) | movements where stock hit zero |
# MAGIC | Min On Hand | MIN(on_hand) | lowest balance reached |
# MAGIC | Units Out | SUM(CASE WHEN delta < 0 THEN -delta ELSE 0 END) | units that left stock (sales) |
# MAGIC | Units In | SUM(CASE WHEN delta > 0 THEN delta ELSE 0 END) | units that entered stock (restocks) |
# MAGIC | Net Stock Change | SUM(delta) | net change across the period |
# MAGIC
# MAGIC **Example.** `inventory_mv` for Ares Depot and PRD-007 shows `Min On Hand` 0 and at least one `Stockout Event`, the incident stockout.
# MAGIC
# MAGIC **Definition of done.**
# MAGIC - `inventory_vw` is one row per stock movement (its count equals `fact_inventory`).
# MAGIC - `inventory_mv` for Ares Depot and PRD-007 shows `Min On Hand` 0 and at least one `Stockout Event`.
# MAGIC
# MAGIC **References.** You may find these Databricks docs helpful:
# MAGIC - [Metric view YAML syntax reference](https://docs.databricks.com/aws/en/business-semantics/metric-views/yaml-reference)
# MAGIC - [Create and edit metric views](https://docs.databricks.com/aws/en/metric-views/create/sql)
# MAGIC - [measure aggregate function](https://docs.databricks.com/aws/en/sql/language-manual/functions/measure)

# COMMAND ----------

# Your turn. Build inventory_vw, then inventory_mv, then run the self check below.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Task 4: Build the inventory combo: inventory_vw and inventory_mv (solution)
# MAGIC <details>
# MAGIC <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; overflow:auto;"></pre>
# MAGIC
# MAGIC <script>
# MAGIC var codeB64 = "IyBTdGVwIDEuIFRoZSByZXBvcnRpbmcgdmlldzogb25lIHJvdyBwZXIgc3RvY2sgbW92ZW1lbnQsIHdpdGggZGVwb3QgYW5kIHByb2R1Y3QgYXR0cmlidXRlcyBqb2luZWQgb24uCnNwYXJrLnNxbChmIiIiCkNSRUFURSBPUiBSRVBMQUNFIFZJRVcge2NhdGFsb2d9LmhlbGlvc19zZW1hbnRpYy5pbnZlbnRvcnlfdncgQVMKU0VMRUNUCiAgaS53YXJlaG91c2VfaWQsIGkucHJvZHVjdF9pZCwgaS5tb3ZlbWVudF90cywgaS5kYXRlX2tleSwgaS5kZWx0YSwgaS5vbl9oYW5kLAogIHcud2FyZWhvdXNlX25hbWUgQVMgZGVwb3QsIHcucmVnaW9uIEFTIGRlcG90X3JlZ2lvbiwKICBwLnByb2R1Y3RfbmFtZSwgYy5jYXRlZ29yeV9uYW1lLCBjLmRlcGFydG1lbnQsIGQuZGF0ZSBBUyBtb3ZlbWVudF9kYXRlCkZST00ge2NhdGFsb2d9LmhlbGlvc19nb2xkLmZhY3RfaW52ZW50b3J5IGkKTEVGVCBKT0lOIHtjYXRhbG9nfS5oZWxpb3NfZ29sZC5kaW1fd2FyZWhvdXNlIHcgT04gaS53YXJlaG91c2VfaWQgPSB3LndhcmVob3VzZV9pZApMRUZUIEpPSU4ge2NhdGFsb2d9LmhlbGlvc19nb2xkLmRpbV9wcm9kdWN0ICAgcCBPTiBpLnByb2R1Y3RfaWQgICA9IHAucHJvZHVjdF9pZApMRUZUIEpPSU4ge2NhdGFsb2d9LmhlbGlvc19nb2xkLmRpbV9jYXRlZ29yeSAgYyBPTiBwLmNhdGVnb3J5X2lkICA9IGMuY2F0ZWdvcnlfaWQKTEVGVCBKT0lOIHtjYXRhbG9nfS5oZWxpb3NfZ29sZC5kaW1fZGF0ZSAgICAgIGQgT04gaS5kYXRlX2tleSAgICAgPSBkLmRhdGVfa2V5CiIiIikKCiMgU3RlcCAyLiBUaGUgbWV0cmljIHZpZXcuIFVuaXRzIE91dCBhbmQgVW5pdHMgSW4gc3BsaXQgdGhlIHNpZ25lZCBkZWx0YSB3aXRoIGEgQ0FTRTsgU3RvY2tvdXQgRXZlbnRzIGNvdW50cyB0aGUKIyBtb3ZlbWVudHMgd2hlcmUgb25faGFuZCBoaXQgemVybzsgTWluIE9uIEhhbmQgaXMgdGhlIGxvd2VzdCBiYWxhbmNlIHJlYWNoZWQuCnNwYXJrLnNxbChmIiIiCkNSRUFURSBPUiBSRVBMQUNFIFZJRVcge2NhdGFsb2d9LmhlbGlvc19zZW1hbnRpYy5pbnZlbnRvcnlfbXYKV0lUSCBNRVRSSUNTCkxBTkdVQUdFIFlBTUwKQVMgJCQKdmVyc2lvbjogMS4xCnNvdXJjZTogIlNFTEVDVCAqIEZST00ge2NhdGFsb2d9LmhlbGlvc19zZW1hbnRpYy5pbnZlbnRvcnlfdnciCmNvbW1lbnQ6ICJIZWxpb3Mgc3RvY2sgbW92ZW1lbnRzIGF0IHRoZSBtb3ZlbWVudCBncmFpbjogdW5pdHMgaW4gYW5kIG91dCwgbmV0IGNoYW5nZSBhbmQgc3RvY2tvdXRzIChvbl9oYW5kIHJlYWNoaW5nIHplcm8pLiBTbGljZSBieSBkZXBvdCwgcHJvZHVjdCwgY2F0ZWdvcnksIGRlcGFydG1lbnQgYW5kIGRhdGUuIE1pbiBPbiBIYW5kIHJldmVhbHMgc3RvY2tvdXRzLiIKZGltZW5zaW9uczoKICAtIG5hbWU6IERlcG90CiAgICBleHByOiBkZXBvdAogIC0gbmFtZTogRGVwb3QgUmVnaW9uCiAgICBleHByOiBkZXBvdF9yZWdpb24KICAtIG5hbWU6IFByb2R1Y3QKICAgIGV4cHI6IHByb2R1Y3RfbmFtZQogIC0gbmFtZTogUHJvZHVjdCBJRAogICAgZXhwcjogcHJvZHVjdF9pZAogIC0gbmFtZTogQ2F0ZWdvcnkKICAgIGV4cHI6IGNhdGVnb3J5X25hbWUKICAtIG5hbWU6IERlcGFydG1lbnQKICAgIGV4cHI6IGRlcGFydG1lbnQKICAtIG5hbWU6IE1vdmVtZW50IERhdGUKICAgIGV4cHI6IG1vdmVtZW50X2RhdGUKICAtIG5hbWU6IElzIFN0b2Nrb3V0CiAgICBleHByOiBvbl9oYW5kID0gMAogICAgY29tbWVudDogIlRydWUgd2hlbiBvbl9oYW5kIHJlYWNoZWQgemVybyAoYSBzdG9ja291dCkiCm1lYXN1cmVzOgogIC0gbmFtZTogTW92ZW1lbnRzCiAgICBleHByOiBDT1VOVCgxKQogIC0gbmFtZTogU3RvY2tvdXQgRXZlbnRzCiAgICBleHByOiBDT1VOVCgxKSBGSUxURVIgKFdIRVJFIG9uX2hhbmQgPSAwKQogIC0gbmFtZTogTWluIE9uIEhhbmQKICAgIGV4cHI6IE1JTihvbl9oYW5kKQogIC0gbmFtZTogVW5pdHMgT3V0CiAgICBleHByOiBTVU0oQ0FTRSBXSEVOIGRlbHRhIDwgMCBUSEVOIC1kZWx0YSBFTFNFIDAgRU5EKQogIC0gbmFtZTogVW5pdHMgSW4KICAgIGV4cHI6IFNVTShDQVNFIFdIRU4gZGVsdGEgPiAwIFRIRU4gZGVsdGEgRUxTRSAwIEVORCkKICAtIG5hbWU6IE5ldCBTdG9jayBDaGFuZ2UKICAgIGV4cHI6IFNVTShkZWx0YSkKJCQKIiIiKQ==";
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
# MAGIC ### Self check
# MAGIC Reconcile the view to the fact, then read `inventory_mv` for the Ares stockout.

# COMMAND ----------

semantic = f"{catalog}.helios_semantic"
view = spark.table(f"{semantic}.inventory_vw")
fact = spark.table(f"{catalog}.helios_gold.fact_inventory")
check("inventory_vw is one row per stock movement", fact.count(), view.count())

ares = spark.sql(f"SELECT MEASURE(`Min On Hand`) min_on_hand, MEASURE(`Stockout Events`) stockouts "
                 f"FROM {semantic}.inventory_mv WHERE `Depot` = 'Ares Depot' AND `Product ID` = 'PRD-007'").first()
check("Ares PRD-007 stockout reaches zero on hand", 0, ares["min_on_hand"])
check_true("Ares PRD-007 has at least one stockout event", ares["stockouts"] >= 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What you built
# MAGIC
# MAGIC The Helios semantic layer in `helios_semantic`, as three combos: `order_lines_vw` feeding `sales_mv`, `orders_vw` feeding `orders_mv`, and `inventory_vw` feeding `inventory_mv`, over the commented and trusted Gold star. Revenue, margin, average order value, cancellation rate, on-time fulfilment and stock health each have one governed definition now. This is the single source that both Genie and the AI/BI dashboards read.