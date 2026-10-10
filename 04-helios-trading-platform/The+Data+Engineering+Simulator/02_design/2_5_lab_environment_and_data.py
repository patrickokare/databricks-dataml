# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Lab 2.5: Set up your environment and land batch one
# MAGIC
# MAGIC ### Ticket: HELIOS-205
# MAGIC
# MAGIC **Context.** Helios Trading Corporation is standing up its first lakehouse. The source feeds and the target lakehouse design are agreed in the design section and written up in `2_2_source_feeds.md` and `2_4_lakehouse_design.md`, in this folder. Before anyone writes a pipeline, every engineer needs a clean, isolated workspace and the agreed source data landed where the pipelines will look for it. You are the engineer picking up the setup card. Everything you build in the upcoming sections starts from what you do here.
# MAGIC
# MAGIC **Task.**
# MAGIC 1. Derive your own catalog from your login and create the medallion schemas.
# MAGIC 2. Create the landing Volume the source feeds drop into.
# MAGIC 3. Run the generator I built for the course to land **batch one** of the source feeds.
# MAGIC
# MAGIC **Acceptance criteria** (the self check cells below):
# MAGIC - A catalog `labs_<your_user>` exists and is in use, with schemas `helios_raw`, `helios_bronze`, `helios_silver`, `helios_gold`, `helios_semantic`.
# MAGIC - A Volume `helios_raw.helios_landing` exists.
# MAGIC - Batch one of the feeds is landed in the Volume: `order_lines`, `orders`, `inventory`, `price_list`, `customers`, `products`, `categories`, `suppliers`, `warehouses`, with the expected row counts. (`returns` arrives from batch two, once orders have been delivered, so it is not present yet.)
# MAGIC
# MAGIC Once the three tasks pass, there is a short Data Exploration activity at the end to get a feel for the raw data.
# MAGIC
# MAGIC > This is the foundation lab, so you write the identity and setup code by hand once to understand it. From Section 3 on, a one line `helios_identity()` from `bootstrap_helpers` does the same thing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bootstrap
# MAGIC
# MAGIC Every lab opens with a bootstrap cell that restores the exact state the lab needs. This is the very first lab, so there is nothing upstream to restore. All the bootstrap does here is load the shared helpers, which gives us the `check()` self check printer the acceptance criteria use. `%run` only defines functions, so it is safe and fast.

# COMMAND ----------

# MAGIC %run ../00_setup/bootstrap_helpers

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Create your catalog and medallion schemas
# MAGIC
# MAGIC Create your own isolated catalog and the five medallion schemas inside it, deriving the catalog name from your login so it is unique to you and every cell is safe to re-run.
# MAGIC
# MAGIC **How.**
# MAGIC - Build the catalog name as `labs_<your_user_id>`. Take your login from `current_user()`, keep the part before the `@`, lowercase it, and replace `.` and `-` with `_` (a catalog name cannot contain `.`, `-` or `@`).
# MAGIC - Create that catalog and switch to it.
# MAGIC - Inside it, create one schema per medallion layer: `helios_raw`, `helios_bronze`, `helios_silver`, `helios_gold`, `helios_semantic`.
# MAGIC - Use `CREATE ... IF NOT EXISTS` throughout so the whole cell is safe to run again.
# MAGIC
# MAGIC **Example.**
# MAGIC
# MAGIC | Your login (`current_user()`) | user_key | catalog |
# MAGIC |---|---|---|
# MAGIC | `jane.doe@helios.com` | `jane_doe` | `labs_jane_doe` |
# MAGIC | `Ana-Maria.Ruiz@helios.com` | `ana_maria_ruiz` | `labs_ana_maria_ruiz` |
# MAGIC
# MAGIC **Definition of done.**
# MAGIC - `catalog` resolves to `labs_<your_user_id>` (for example `labs_jane_doe`) and is the catalog in use.
# MAGIC - All five schemas exist: `helios_raw`, `helios_bronze`, `helios_silver`, `helios_gold`, `helios_semantic`.
# MAGIC - Re-running the whole cell completes without error.

# COMMAND ----------

# Your turn. Set `catalog` and create the catalog + the five helios_ schemas.
# (Try it before opening the reveal.)

x = spark.sql("SELECT current_user()").first()[0]
user_key = x.split('@')[0].lower()
user_key = user_key.replace('.','_')
user_key = user_key.replace('-','_')

catalog = f"labs_{user_key}"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}");
spark.sql(f"USE CATALOG {catalog}");
# spark.sql("CREATE SCHEMA IF NOT EXISTS helios_raw ");
# spark.sql("CREATE SCHEMA IF NOT EXISTS helios_bronze ");
# spark.sql("CREATE SCHEMA IF NOT EXISTS helios_silver");
# spark.sql("CREATE SCHEMA IF NOT EXISTS helios_gold");
# spark.sql("CREATE SCHEMA IF NOT EXISTS helios_semantic");

for table_name in ['helios_raw', 'helios_bronze', 'helios_silver', 'helios_gold', 'helios_semantic']:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{table_name}")


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Task 1: Create your catalog and medallion schemas (solution)
# MAGIC <details>
# MAGIC <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; overflow:auto;"></pre>
# MAGIC
# MAGIC <script>
# MAGIC var codeB64 = "dXNlcm5hbWUgPSBzcGFyay5zcWwoIlNFTEVDVCBjdXJyZW50X3VzZXIoKSIpLmZpcnN0KClbMF0KdXNlcl9rZXkgPSB1c2VybmFtZS5zcGxpdCgiQCIpWzBdLnJlcGxhY2UoIi4iLCAiXyIpLnJlcGxhY2UoIi0iLCAiXyIpLmxvd2VyKCkKY2F0YWxvZyAgPSBmImxhYnNfe3VzZXJfa2V5fSIKCnNwYXJrLnNxbChmIkNSRUFURSBDQVRBTE9HIElGIE5PVCBFWElTVFMge2NhdGFsb2d9IikKc3Bhcmsuc3FsKGYiVVNFIENBVEFMT0cge2NhdGFsb2d9IikKCmZvciBsYXllciBpbiBbImhlbGlvc19yYXciLCAiaGVsaW9zX2Jyb256ZSIsICJoZWxpb3Nfc2lsdmVyIiwgImhlbGlvc19nb2xkIiwgImhlbGlvc19zZW1hbnRpYyJdOgogICAgc3Bhcmsuc3FsKGYiQ1JFQVRFIFNDSEVNQSBJRiBOT1QgRVhJU1RTIHtjYXRhbG9nfS57bGF5ZXJ9IikKCnByaW50KCJDYXRhbG9nIGluIHVzZToiLCBjYXRhbG9nKQ==";
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

expected_schemas = {"helios_raw", "helios_bronze", "helios_silver", "helios_gold", "helios_semantic"}

current_catalog = spark.sql("SELECT current_catalog()").first()[0]
actual_schemas = {
    r.schema_name
    for r in spark.sql(f"SELECT schema_name FROM {catalog}.information_schema.schemata").collect()
}

check("catalog follows the labs_<user> convention", True, current_catalog.startswith("labs_"))
check("catalog is in use", catalog, current_catalog)
check("all five medallion schemas exist", expected_schemas, expected_schemas & actual_schemas)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Create the landing Volume
# MAGIC
# MAGIC Create the Unity Catalog Volume your source files will land in: `helios_landing`, inside the `helios_raw` schema.
# MAGIC
# MAGIC **How.**
# MAGIC - On this course the source files go into a Unity Catalog Volume, which keeps the raw files under the same governance as your tables (permissions, lineage and auditing).
# MAGIC - Its path is `/Volumes/<catalog>/helios_raw/helios_landing`, where the generator writes in the next task and where Auto Loader and Lakeflow read in later sections.
# MAGIC - Use `IF NOT EXISTS` so the cell is safe to re-run.
# MAGIC
# MAGIC **Example.** For catalog `labs_jane_doe` the Volume path is `/Volumes/labs_jane_doe/helios_raw/helios_landing`.
# MAGIC
# MAGIC **Definition of done.**
# MAGIC - A Volume named `helios_landing` exists in the `helios_raw` schema.
# MAGIC - Re-running the cell completes without error.

# COMMAND ----------

# Your turn. Create the landing Volume helios_raw.helios_landing.
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.helios_raw.helios_landing")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Task 2: Create the landing Volume (solution)
# MAGIC <details>
# MAGIC <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; overflow:auto;"></pre>
# MAGIC
# MAGIC <script>
# MAGIC var codeB64 = "c3Bhcmsuc3FsKGYiQ1JFQVRFIFZPTFVNRSBJRiBOT1QgRVhJU1RTIHtjYXRhbG9nfS5oZWxpb3NfcmF3LmhlbGlvc19sYW5kaW5nIikKcHJpbnQoIkxhbmRpbmcgVm9sdW1lOiIsIGYiL1ZvbHVtZXMve2NhdGFsb2d9L2hlbGlvc19yYXcvaGVsaW9zX2xhbmRpbmciKQ==";
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

volumes = {
    r.volume_name
    for r in spark.sql(
        f"SELECT volume_name FROM {catalog}.information_schema.volumes "
        f"WHERE volume_schema = 'helios_raw'"
    ).collect()
}
check_true("landing Volume helios_landing exists", "helios_landing" in volumes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Land batch one of the source feeds
# MAGIC
# MAGIC Run the course generator to land **batch one** into your Volume. You do not write the generator, you run it, the same way you would point a pipeline at a real source system.
# MAGIC
# MAGIC **How.**
# MAGIC - The generator stands in for the upstream source systems (the depot point of sale and warehouse systems, the supplier price feeds, the customer registry). It lives at `00_setup/data_generator`.
# MAGIC - `%run` the generator notebook (cell below) to bring its functions into scope.
# MAGIC - This is the foundation lab, so start from an empty landing zone with `clear_landing(catalog)`, then land batch one with `generate_batch(catalog, batch=1)`. Clearing first means you always land exactly one clean batch, whatever a previous run left behind.
# MAGIC
# MAGIC **About the generator** (worth knowing as you run it):
# MAGIC - It produces the feeds in the formats the source feeds design specifies: `order_lines`, `orders`, `suppliers` and `warehouses` as JSON, `inventory` as JSON, `customers`, `products` and `categories` as Parquet, `price_list` as CSV.
# MAGIC - It is deterministic. A fixed seed means everyone lands identical data, so the self checks and the reveal solutions always line up.
# MAGIC - It takes a batch number. Batch one is the clean initial load; later sections call higher batches to replay the order lifecycle, SCD Type 2 price and tier changes, the return wave, and late and duplicate records.
# MAGIC - Batch one deliberately includes some duplicate submissions and a few non positive quantities, so the Silver layer you build in Section 3 has real mess to clean up. `returns` is empty on batch one (nothing has been delivered yet), so it is not landed until batch two.
# MAGIC
# MAGIC **Example of what lands** (each feed in its own folder under the landing Volume):
# MAGIC
# MAGIC | Feed | Folder | Format | Rows |
# MAGIC |---|---|---|---|
# MAGIC | order_lines | `order_lines/batch_1/` | JSON | 50,626 (about 49,000 clean + ~1,000 duplicates + ~500 dirty) |
# MAGIC | orders | `orders/batch_1/` | JSON | 45,628 (CDC changes) |
# MAGIC | inventory | `inventory/batch_1/` | JSON | 46,575 |
# MAGIC | price_list | `price_list/batch_1/` | CSV | 150 |
# MAGIC | customers | `customers/snapshot_2257-03-01/` | Parquet | 300 |
# MAGIC | products | `products/batch_1/` | Parquet | 150 |
# MAGIC | categories | `categories/batch_1/` | Parquet | 7 |
# MAGIC | suppliers | `suppliers/batch_1/` | JSON | 80 |
# MAGIC | warehouses | `warehouses/batch_1/` | JSON | 6 |
# MAGIC
# MAGIC **Definition of done.**
# MAGIC - The feeds above are landed under `/Volumes/<catalog>/helios_raw/helios_landing`, each in its own folder.
# MAGIC - The row counts read back exactly as above (order_lines 50,626, orders 45,628, inventory 46,575, price_list 150, customers 300, products 150, categories 7, suppliers 80, warehouses 6).
# MAGIC - `returns` is not present yet (it lands from batch two).
# MAGIC - The landing zone holds only batch one (you cleared it first), so re-running lands the same files, not more.

# COMMAND ----------

# MAGIC %run ../00_setup/data_generator

# COMMAND ----------

# Your turn. Start the landing zone clean, then land batch one.

# clear_landing(catalog)
# print("Landing cleared successfully");

generate_batch(catalog, batch=1)
print("Batch 1 generated successfully");


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Task 3: Land batch one of the source feeds (solution)
# MAGIC <details>
# MAGIC <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; overflow:auto;"></pre>
# MAGIC
# MAGIC <script>
# MAGIC var codeB64 = "Y2xlYXJfbGFuZGluZyhjYXRhbG9nKSAgICAgICAgICAgICAjIGZvdW5kYXRpb24gbGFiOiBzdGFydCBmcm9tIGFuIGVtcHR5IGxhbmRpbmcgem9uZQpnZW5lcmF0ZV9iYXRjaChjYXRhbG9nLCBiYXRjaD0xKQ==";
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
# MAGIC
# MAGIC Reads the files straight back out of the Volume and compares the row counts to the batch one numbers in the ticket, so a PASS means the data is really on disk and readable, not just that the call returned.

# COMMAND ----------

root = f"/Volumes/{catalog}/helios_raw/helios_landing"

# Batch one lands these row counts (see the ticket). recursiveFileLookup descends into each feed's per-batch
# subfolder; because Task 3 cleared the zone first, only batch one is present, so the counts match exactly.
expected = {"order_lines": 50626, "orders": 45628, "inventory": 46575, "price_list": 150,
            "customers": 300, "products": 150, "categories": 7, "suppliers": 80, "warehouses": 6}

def read(feed, fmt):
    reader = spark.read.option("recursiveFileLookup", "true")
    if fmt == "csv":
        reader = reader.option("header", "true")
    return reader.format(fmt).load(f"{root}/{feed}").count()

formats = {"order_lines": "json", "orders": "json", "inventory": "json", "price_list": "csv",
           "customers": "parquet", "products": "parquet", "categories": "parquet",
           "suppliers": "json", "warehouses": "json"}
landed = {feed: read(feed, fmt) for feed, fmt in formats.items()}

for feed, count in expected.items():
    check(f"{feed} landed and reads back", count, landed[feed])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Exploration
# MAGIC
# MAGIC The data is landed, so take a moment to look around before Section 3 starts building on it. Read a few feeds into DataFrames and inspect them with `display()` and `printSchema()`. There is no self check here, this is just to get a feel for the raw shape, but pointing Spark at JSON, Parquet and CSV is a core skill, so write it yourself before opening the reveal.
# MAGIC
# MAGIC The generator lands each feed under a per-batch subfolder (e.g. `order_lines/batch_1/`), so pass `recursiveFileLookup` so Spark descends into it; without it a plain read sees only the subfolder and cannot infer a schema. In Section 3, Auto Loader does this descent for you.
# MAGIC
# MAGIC You will notice the raw data is messy: some `order_line_id` values repeat, a few quantities are zero or negative, and the JSON and CSV feeds arrive with every column typed as a string. That is expected and fine for now. Turning this raw drop into clean, typed, deduplicated tables is exactly the job you pick up in Section 3.

# COMMAND ----------

# Your turn. Read a few feeds (JSON, Parquet, CSV) into DataFrames and look at them with
# display() and printSchema(). Then open the reveal to compare.
dataset = ['categories', 'customers', 'products', 'inventory', 'order_lines', 'orders', 'price_list',  'suppliers', 'warehouses']
root = f'/Volumes/labs_waleokare/helios_raw/helios_landing'

# --- Largest feeds first (descending by row count) ---

# order_lines: 50,626 rows (JSON)
order_lines_df = spark.read.option('recursiveFileLookup', 'true').json(f'{root}/{dataset[4]}')
# order_lines_df.printSchema()

# inventory: 46,575 rows (JSON)
inventory_df = spark.read.option('recursiveFileLookup', 'true').json(f'{root}/{dataset[3]}')

# orders: 45,628 rows (JSON)
orders_df = spark.read.option('recursiveFileLookup', 'true').json(f'{root}/{dataset[5]}')

# customers: 300 rows (Parquet)
customers_df = spark.read.option('recursiveFileLookup', 'true').parquet(f'{root}/{dataset[1]}')

# price_list: 150 rows (CSV)
price_list_df = spark.read.option('recursiveFileLookup', 'true').option('header', 'true').csv(f'{root}/{dataset[6]}')

# products: 150 rows (Parquet)
products_df = spark.read.option('recursiveFileLookup', 'true').parquet(f'{root}/{dataset[2]}')

# suppliers: 80 rows (JSON)
suppliers_df = spark.read.option('recursiveFileLookup', 'true').json(f'{root}/{dataset[7]}')

# categories: 7 rows (Parquet)
categories_df = spark.read.option('recursiveFileLookup', 'true').parquet(f'{root}/{dataset[0]}')

# warehouses: 6 rows (JSON)
warehouses_df = spark.read.option('recursiveFileLookup', 'true').json(f'{root}/{dataset[8]}')

# display(orders_df.limit(50))

# COMMAND ----------

warehouses_df.createOrReplaceTempView('vw_dataset')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from vw_dataset
# MAGIC -- where customer_id = 'CUST-0001'
# MAGIC -- LIMIT 100

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Data Exploration (solution)
# MAGIC <details>
# MAGIC <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; overflow:auto;"></pre>
# MAGIC
# MAGIC <script>
# MAGIC var codeB64 = "cm9vdCA9IGYiL1ZvbHVtZXMve2NhdGFsb2d9L2hlbGlvc19yYXcvaGVsaW9zX2xhbmRpbmciCgojIEZpbGVzIGxhbmQgdW5kZXIgYSBwZXItYmF0Y2ggc3ViZm9sZGVyLCBzbyByZWFkIHdpdGggcmVjdXJzaXZlRmlsZUxvb2t1cCB0byBkZXNjZW5kIGludG8gaXQuCm9yZGVyX2xpbmVzID0gc3BhcmsucmVhZC5vcHRpb24oInJlY3Vyc2l2ZUZpbGVMb29rdXAiLCAidHJ1ZSIpLmpzb24oZiJ7cm9vdH0vb3JkZXJfbGluZXMiKQpvcmRlcnMgICAgICA9IHNwYXJrLnJlYWQub3B0aW9uKCJyZWN1cnNpdmVGaWxlTG9va3VwIiwgInRydWUiKS5qc29uKGYie3Jvb3R9L29yZGVycyIpCmN1c3RvbWVycyAgID0gc3BhcmsucmVhZC5vcHRpb24oInJlY3Vyc2l2ZUZpbGVMb29rdXAiLCAidHJ1ZSIpLnBhcnF1ZXQoZiJ7cm9vdH0vY3VzdG9tZXJzIikKcHJpY2VfbGlzdCAgPSBzcGFyay5yZWFkLm9wdGlvbigicmVjdXJzaXZlRmlsZUxvb2t1cCIsICJ0cnVlIikub3B0aW9uKCJoZWFkZXIiLCAidHJ1ZSIpLmNzdihmIntyb290fS9wcmljZV9saXN0IikKCiMgTG9vayBhdCB0aGUgaGVybyBmYWN0LCB0aGVuIGNvbXBhcmUgc2NoZW1hcy4KZGlzcGxheShvcmRlcl9saW5lcykKCiMgSlNPTiBhbmQgQ1NWIGxhbmQgZXZlcnl0aGluZyBhcyBzdHJpbmdzOyBQYXJxdWV0IGtlZXBzIHJlYWwgdHlwZXMuCm9yZGVyX2xpbmVzLnByaW50U2NoZW1hKCkKcHJpY2VfbGlzdC5wcmludFNjaGVtYSgp";
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
# MAGIC ## What you built
# MAGIC
# MAGIC - Your own isolated catalog `labs_<user>` with the five medallion schemas.
# MAGIC - The governed landing Volume `helios_raw.helios_landing`.
# MAGIC - Batch one of the source feeds, landed exactly as `2_2_source_feeds.md` specifies.
# MAGIC
# MAGIC This is the canonical base every later section starts from. Section 3 builds the medallion over this data imperatively with Jobs; Section 4 rebuilds it declaratively with Lakeflow. Both read the same files you just landed.