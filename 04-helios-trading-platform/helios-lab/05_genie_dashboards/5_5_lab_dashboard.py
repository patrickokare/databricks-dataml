# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 5.5: Build an AI/BI Dashboard on the semantic layer
# MAGIC
# MAGIC ### Ticket: HELIOS-505
# MAGIC
# MAGIC **Context.** The same metric views that answer questions in Genie also draw the dashboard, so the numbers on a chart match the numbers Genie quotes. Because the metrics are already defined in the views, you write no SQL here: you add each metric view as a dataset, and build every widget by picking its dimensions and measures. You organise the result into three topic pages, one per metric view, so each page is coherent and any filter you add reaches every widget on it: Sales Performance reads `sales_mv`, Fulfilment reads `orders_mv`, and Inventory reads `inventory_mv`.
# MAGIC
# MAGIC **Task.**
# MAGIC 1. Create the dashboard and add the three metric views as datasets.
# MAGIC 2. Build three named topic pages (Sales Performance, Fulfilment, Inventory), each built from a single metric view, with a small KPI row and the visuals that tell its story.
# MAGIC
# MAGIC **Acceptance criteria** (you confirm these in the UI):
# MAGIC - The dashboard has `sales_mv`, `orders_mv` and `inventory_mv` as datasets.
# MAGIC - It has three named pages, each built from one metric view: Sales Performance (`sales_mv`), Fulfilment (`orders_mv`) and Inventory (`inventory_mv`).
# MAGIC - Every widget is built by picking a metric view's dimensions and measures (no SQL).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bootstrap
# MAGIC Runs the shared `genie_bootstrap` helper (it rebuilds the canonical Gold and the Lab 5.3 semantic layer), so the metric views the dashboard reads are present. It is idempotent.

# COMMAND ----------

# MAGIC %run ../00_setup/genie_bootstrap

# COMMAND ----------

# Confirm the three metric views have data before you build. You will NOT write SQL like this on the dashboard;
# you add the metric view as a dataset and it applies MEASURE() for you. This cell is only a quick data check.
semantic = f"{catalog}.helios_semantic"
for view, measure in [("sales_mv", "Revenue"), ("orders_mv", "Orders"), ("inventory_mv", "Movements")]:
    total = spark.sql(f"SELECT MEASURE(`{measure}`) v FROM {semantic}.{view}").first()["v"]
    print(f"{view}: {measure} = {total:,}")
print("Your catalog:", catalog)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your task: Build the dashboard
# MAGIC
# MAGIC This is a UI lab. You build the dashboard in the AI/BI dashboard editor; there is no SQL to write, because the metric views already carry the measures. The reveals hold the dataset names to add and the field choices for each page and widget.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Create the dashboard and add the metric views as datasets
# MAGIC
# MAGIC Create a new AI/BI dashboard and add your three metric views as datasets. Adding a metric view directly is the point of the semantic layer: the dataset carries all the governed dimensions and measures, so every widget reads the one definition and no widget can drift from Genie.
# MAGIC
# MAGIC **How.**
# MAGIC - In the workspace, open **Dashboards**, then **Create dashboard**. Name it "Helios Trading Executive Performance".
# MAGIC - Open the **Data** tab, click **Add data**, and choose a Unity Catalog metric view. Add `helios_semantic.sales_mv`, then `orders_mv`, then `inventory_mv` (one dataset each). Choosing a metric view directly brings in all of its dimensions and measures; you do not type a query.
# MAGIC - That is all the data setup. You do not write `MEASURE()` here; the dashboard applies it when you use a measure in a widget.
# MAGIC
# MAGIC **Example.** After adding `sales_mv`, its dataset lists dimensions like Depot and Category and measures like Revenue and Gross Margin, ready to drop onto a chart.
# MAGIC
# MAGIC **Definition of done.**
# MAGIC - The dashboard has three datasets, `sales_mv`, `orders_mv` and `inventory_mv`, each added as a metric view (no SQL query).
# MAGIC
# MAGIC **References.** You may find these Databricks docs helpful:
# MAGIC - [Create a dashboard](https://docs.databricks.com/aws/en/dashboards/tutorials/create-dashboard)
# MAGIC - [Create and manage dashboard datasets](https://docs.databricks.com/aws/en/dashboards/manage/data-modeling/datasets)
# MAGIC - [Query metric views](https://docs.databricks.com/aws/en/business-semantics/metric-views/query)
# MAGIC - [Local metric views](https://docs.databricks.com/aws/en/dashboards/manage/data-modeling/local-metric-views)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Task 1: Datasets to add (solution)
# MAGIC <details>
# MAGIC <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; overflow:auto;"></pre>
# MAGIC
# MAGIC <script>
# MAGIC var codeB64 = "QWRkIHRoZXNlIHRocmVlIFVuaXR5IENhdGFsb2cgbWV0cmljIHZpZXdzIGFzIGRhdGFzZXRzIChEYXRhIHRhYiwgdGhlbiBBZGQgZGF0YSwgdGhlbiBjaG9vc2UgdGhlIG1ldHJpYyB2aWV3KToKCmhlbGlvc19zZW1hbnRpYy5zYWxlc19tdgpoZWxpb3Nfc2VtYW50aWMub3JkZXJzX212CmhlbGlvc19zZW1hbnRpYy5pbnZlbnRvcnlfbXYKCkVhY2ggb25lIGJyaW5ncyBpbiBhbGwgb2YgaXRzIGRpbWVuc2lvbnMgYW5kIG1lYXN1cmVzLiBZb3UgZG8gbm90IHdyaXRlIGEgcXVlcnkgYW5kIHlvdSBkbyBub3QgY2FsbCBNRUFTVVJFKCk7IHRoZSBkYXNoYm9hcmQgYXBwbGllcyBpdCBmb3IgeW91IHdoZW4geW91IHVzZSBhIG1lYXN1cmUgaW4gYSB3aWRnZXQu";
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
# MAGIC ## Task 2: Build three topic pages
# MAGIC
# MAGIC Organise the dashboard into three named pages, one per metric view, so each reads cleanly and stays coherent. A dashboard filter only reaches the datasets that contain the field, so keeping a page to a single metric view means any Depot or date filter you add there covers its counters, its chart and its table together, with no per-widget wiring. AI/BI dashboards have pages (tabs along the bottom); add a page with the plus control and give each one its name. On each page pick the visual that fits the story: an area or line coloured by depot for a trend, a combo for two measures over time, a ranked bar for a comparison, a table for the detail. Then set each widget's sort, colour and number format so it reads at a glance. You build every widget by choosing its dataset and its fields; the reveal has the exact fields, sort and colour for each.
# MAGIC
# MAGIC **How.** Build these three single-view pages (the reveal has the full per-widget settings):
# MAGIC
# MAGIC **Page 1: Sales Performance** (all from `sales_mv`).  How revenue is trending, which depots drive it, which products sell, and how much cancellations take out (gross vs net revenue).
# MAGIC
# MAGIC | Widget | Type | Fields, sort, colour |
# MAGIC |---|---|---|
# MAGIC | Total revenue | Counter | Revenue, as CREDITS |
# MAGIC | Net revenue | Counter | Net Revenue (excludes cancelled orders), as CREDITS |
# MAGIC | Gross margin rate | Counter | Gross Margin Rate, as a percent |
# MAGIC | Return rate | Counter | Return Rate, as a percent |
# MAGIC | Revenue by depot over time | Area (stacked, coloured by depot) | X = Order Date, Y = Revenue, colour = Depot; sort by date |
# MAGIC | Gross vs net revenue over time | Line (two measures) | X = Order Date, Y = Revenue and Net Revenue; the lines separate on 2257-03-03 as cancellations spike |
# MAGIC | Products by revenue | Table | Product, Units Sold, Average Unit Price, Gross Margin Rate, Revenue; sort Revenue descending |
# MAGIC | Revenue by product | Bar (horizontal) | Product vs Revenue; sort Revenue descending |
# MAGIC
# MAGIC **Page 2: Fulfilment** (all from `orders_mv`).  Order volume, cancellations, and the depot the incident hit.
# MAGIC
# MAGIC | Widget | Type | Fields, sort, colour |
# MAGIC |---|---|---|
# MAGIC | Number of orders | Counter | Orders |
# MAGIC | Cancellation rate | Counter | Cancellation Rate, percent |
# MAGIC | On-time fulfilment rate | Counter | On Time Fulfilment Rate, percent |
# MAGIC | Daily orders and cancellation rate | Combo (bars + line, dual axis) | X = Order Date; bars = Orders (left), line = Cancellation Rate (right, percent); sort by date |
# MAGIC | Cancellation rate by depot over time | Line (coloured by depot) | X = Order Date, Y = Cancellation Rate, colour = Depot; Ares Depot spikes on 2257-03-03 |
# MAGIC
# MAGIC **Page 3: Inventory** (all from `inventory_mv`).  How much stock moves and where it runs out.
# MAGIC
# MAGIC | Widget | Type | Fields, sort, colour |
# MAGIC |---|---|---|
# MAGIC | Stock movements | Counter | Movements |
# MAGIC | Stockouts | Counter | Stockout Events |
# MAGIC | Units shipped out | Counter | Units Out |
# MAGIC | Units out by depot over time | Area (stacked, coloured by depot) | X = Movement Date, Y = Units Out, colour = Depot; sort by date |
# MAGIC | Stockout watch | Table | Depot, Min On Hand, Stockout Events; sort Stockout Events descending |
# MAGIC
# MAGIC **Example.** On the Fulfilment page, the cancellation-rate line holds flat near 2% to 5% for five depots while Ares Depot spikes to about 9.7% on 2257-03-03; on the Inventory page, the Stockout watch table shows Europa Outpost leading with twelve stockouts and Luna Hub the only depot that never ran out.
# MAGIC
# MAGIC **Definition of done.**
# MAGIC - Three named pages exist (Sales Performance, Fulfilment, Inventory), each built from a single metric view (`sales_mv`, `orders_mv`, `inventory_mv`), all from picking fields (no SQL).
# MAGIC - Sales Performance has four counters (including Net Revenue), a stacked area of revenue by depot over time, a gross-vs-net revenue line, and a product ranking (a table and a bar), sorted by revenue.
# MAGIC - Fulfilment has three counters, the dual-axis combo (orders bars and cancellation rate line) and the cancellation-rate line coloured by depot (Ares spikes on 2257-03-03).
# MAGIC - Inventory has three counters, a stacked area of units out by depot over time, and a Stockout watch table by depot (Min On Hand and Stockout Events).
# MAGIC - Rate counters display as percents, and every chart has its sort and colour set.
# MAGIC
# MAGIC **References.** You may find these Databricks docs helpful:
# MAGIC - [Dashboard visualizations](https://docs.databricks.com/aws/en/dashboards/manage/visualizations/)
# MAGIC - [AI/BI dashboard visualization types](https://docs.databricks.com/aws/en/dashboards/manage/visualizations/types)
# MAGIC - [Dashboards](https://docs.databricks.com/aws/en/dashboards/)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Task 2: Page and widget field choices (solution)
# MAGIC <details>
# MAGIC <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre; overflow:auto;"></pre>
# MAGIC
# MAGIC <script>
# MAGIC var codeB64 = "QnVpbGQgZWFjaCBwYWdlIGZyb20gYSBTSU5HTEUgbWV0cmljIHZpZXcuIEEgZGFzaGJvYXJkIGZpbHRlciBvbmx5IHJlYWNoZXMgdGhlIGRhdGFzZXRzIHRoYXQgY29udGFpbiB0aGUgZmllbGQsIHNvIHdoZW4gZXZlcnkgd2lkZ2V0IG9uIGEgcGFnZSByZWFkcyB0aGUgc2FtZSBtZXRyaWMgdmlldywgb25lIERlcG90IG9yIGRhdGUgZmlsdGVyIG9uIHRoYXQgcGFnZSB3b3VsZCBjb3ZlciB0aGVtIGFsbC4gVGhhdCBpcyB3aHkgU2FsZXMgUGVyZm9ybWFuY2UgaXMgYWxsIHNhbGVzX212LCBGdWxmaWxtZW50IGlzIGFsbCBvcmRlcnNfbXYsIGFuZCBJbnZlbnRvcnkgaXMgYWxsIGludmVudG9yeV9tdi4KCkFkZCBhIHBhZ2UgcGVyIHRvcGljICh0aGUgdGFicyBzaXQgYWxvbmcgdGhlIGJvdHRvbTsgYWRkIG9uZSB3aXRoIHRoZSBwbHVzIGNvbnRyb2wsIHRoZW4gZG91YmxlLWNsaWNrIHRoZSB0YWIgdG8gcmVuYW1lIGl0KS4gQnVpbGQgZWFjaCB3aWRnZXQgYnkgY2hvb3NpbmcgaXRzIGRhdGFzZXQgKHRoZSBwYWdlJ3MgbWV0cmljIHZpZXcpLCB0aGVuIHRoZSB2aXN1YWxpemF0aW9uIHR5cGUsIHRoZW4gaXRzIGZpZWxkcywgdGhlbiBzZXQgdGhlIHNvcnQsIGNvbG91ciBhbmQgbnVtYmVyIGZvcm1hdC4gVG8gYnJlYWsgYSBtZWFzdXJlIGRvd24gYnkgZGVwb3QsIHVzZSBhIGxpbmUgb3IgYW4gYXJlYSBhbmQgc2V0IGl0cyBjb2xvdXIgdG8gdGhlIERlcG90IGZpZWxkOyBrZWVwIGEgY29sb3VyIHNwbGl0IHRvIGVpZ2h0IGNhdGVnb3JpZXMgb3IgZmV3ZXIgKHNpeCBkZXBvdHMgaXMgZmluZSkuCgoKUEFHRSAxLCBuYW1lIGl0ICJTYWxlcyBQZXJmb3JtYW5jZSIuICBNZXRyaWMgdmlldzogc2FsZXNfbXYuICBTdG9yeTogaG93IHJldmVudWUgaXMgdHJlbmRpbmcsIHdoaWNoIGRlcG90cyBkcml2ZSBpdCBhbmQgd2hpY2ggcHJvZHVjdHMgc2VsbC4KQ291bnRlcnMgKGEgbWVhc3VyZSB3aXRoIG5vIGRpbWVuc2lvbiBpcyB0aGUgZ3JhbmQgdG90YWwpOgotIFRvdGFsIHJldmVudWU6IHNhbGVzX212LCBSZXZlbnVlLiBXaG9sZSBudW1iZXIsIENSRURJVFMsIGNvbXBhY3QgKGFib3V0IDEuODZibikuCi0gTmV0IHJldmVudWU6IHNhbGVzX212LCBOZXQgUmV2ZW51ZS4gV2hvbGUgbnVtYmVyLCBDUkVESVRTLCBjb21wYWN0LiBSZXZlbnVlIHdpdGggY2FuY2VsbGVkLW9yZGVyIGxpbmVzIHJlbW92ZWQsIHNvIGl0IHNpdHMgYSBsaXR0bGUgYmVsb3cgVG90YWwgcmV2ZW51ZTsgdGhlIGdhcCBpcyB0aGUgY2FuY2VsbGVkIGJvb2tpbmdzLgotIEdyb3NzIG1hcmdpbiByYXRlOiBzYWxlc19tdiwgR3Jvc3MgTWFyZ2luIFJhdGUuIFBlcmNlbnQsIG9uZSBkZWNpbWFsIChhYm91dCA0MCUpLgotIFJldHVybiByYXRlOiBzYWxlc19tdiwgUmV0dXJuIFJhdGUuIFBlcmNlbnQsIHR3byBkZWNpbWFscyAoYWJvdXQgMC40MyUpLgoKQXJlYSBjaGFydCAoc3RhY2tlZCwgY29sb3VyZWQgYnkgZGVwb3QpLCBSZXZlbnVlIGJ5IGRlcG90IG92ZXIgdGltZToKLSBYID0gT3JkZXIgRGF0ZSwgWSA9IFJldmVudWUsIGNvbG91ciA9IERlcG90IChzaXggZGVwb3RzKS4KLSBTb3J0IFggYnkgZGF0ZSBhc2NlbmRpbmc7IGZvcm1hdCBZIGFzIENSRURJVFMgY29tcGFjdDsgY29sb3VyIHNjYWxlIGNhdGVnb3JpY2FsLgotIFJlYWRzOiB0b3RhbCByZXZlbnVlIGNsaW1icyB0byBhIHBlYWsgb24gMjI1Ny0wMy0wMyAoYWJvdXQgNDQxTSksIGFuZCBlYWNoIGNvbG91ciBiYW5kIGlzIGEgZGVwb3QncyBzaGFyZSwgc28geW91IHNlZSB0aGUgdHJlbmQgYW5kIHRoZSBkZXBvdCBtaXggaW4gb25lIGNoYXJ0LgoKTGluZSBjaGFydCAodHdvIG1lYXN1cmVzKSwgR3Jvc3MgdnMgbmV0IHJldmVudWUgb3ZlciB0aW1lOgotIFggPSBPcmRlciBEYXRlOyBZID0gUmV2ZW51ZSBhbmQgTmV0IFJldmVudWUgKHR3byBsaW5lcyBvbiBvbmUgYXhpcykuCi0gU29ydCBYIGJ5IGRhdGUgYXNjZW5kaW5nOyBmb3JtYXQgWSBhcyBDUkVESVRTIGNvbXBhY3Q7IGdpdmUgUmV2ZW51ZSBhIG5ldXRyYWwgYmx1ZSBhbmQgTmV0IFJldmVudWUgYSBjb250cmFzdGluZyBjb2xvdXIuCi0gUmVhZHM6IHRoZSB0d28gbGluZXMgdHJhY2sgY2xvc2VseSBtb3N0IGRheXMgKGEgc3RlYWR5IGZldyBwZXJjZW50IG9mIGJvb2tpbmdzIGNhbmNlbCksIHRoZW4gc2VwYXJhdGUgb24gMjI1Ny0wMy0wMyB3aGVuIHRoZSBBcmVzIG1pc3ByaWNpbmcgdHJpZ2dlcnMgYSBjYW5jZWxsYXRpb24gd2F2ZSwgc28gdGhlIGdhcCBiZXR3ZWVuIGdyb3NzIGJvb2tpbmdzIGFuZCBuZXQgcmV2ZW51ZSB3aWRlbnMgb24gdGhlIGluY2lkZW50IGRheS4gVGhpcyBpcyB0aGUgc2FsZXMtc2lkZSB2aWV3IG9mIHRoZSBpbmNpZGVudCB0aGUgRnVsZmlsbWVudCBwYWdlIHNob3dzIGFzIGEgY2FuY2VsbGF0aW9uIHNwaWtlLgoKVGFibGUsIFByb2R1Y3RzIGJ5IHJldmVudWU6Ci0gcm93cyA9IFByb2R1Y3Q7IGNvbHVtbnMgPSBVbml0cyBTb2xkLCBBdmVyYWdlIFVuaXQgUHJpY2UsIEdyb3NzIE1hcmdpbiBSYXRlLCBSZXZlbnVlLiBTb3J0IFJldmVudWUgZGVzY2VuZGluZy4KLSBLZWVwIHRoZSBHcm9zcyBNYXJnaW4gUmF0ZSBjb2x1bW46IHRoZSBtaXNwcmljZWQgcGFydCBQUkQtMDA3IHNob3dzIGEgbmVnYXRpdmUgbWFyZ2luIHRoZXJlIHdoaWxlIGl0cyBwcm9wdWxzaW9uIHBlZXJzIHNpdCBuZWFyIHBsdXMgNDUlLCBzbyB0aGUgdGFibGUgaXMgd2hlcmUgdGhlIGluY2lkZW50IHBhcnQgZ2l2ZXMgaXRzZWxmIGF3YXkuCgpCYXIgY2hhcnQgKGhvcml6b250YWwpLCBSZXZlbnVlIGJ5IHByb2R1Y3Q6Ci0gWSA9IFByb2R1Y3QsIFggPSBSZXZlbnVlLiBTb3J0IFJldmVudWUgZGVzY2VuZGluZzsgYSBzaW5nbGUgYmx1ZS4KLSBSZWFkczogdGhlIHByb2R1Y3RzIHJhbmtlZCBieSByZXZlbnVlLCB0aGUgcHJvcHVsc2lvbiB1bml0cyBmaXJzdC4KCgpQQUdFIDIsIG5hbWUgaXQgIkZ1bGZpbG1lbnQiLiAgTWV0cmljIHZpZXc6IG9yZGVyc19tdi4gIFN0b3J5OiBvcmRlciB2b2x1bWUsIGhvdyBtYW55IGNhbmNlbCwgYW5kIHdoaWNoIGRlcG90IHRoZSBpbmNpZGVudCBoaXQuCkNvdW50ZXJzOgotIE51bWJlciBvZiBvcmRlcnM6IG9yZGVyc19tdiwgT3JkZXJzLiBXaG9sZSBudW1iZXIgKGFib3V0IDk1LDEwMCkuCi0gQ2FuY2VsbGF0aW9uIHJhdGU6IG9yZGVyc19tdiwgQ2FuY2VsbGF0aW9uIFJhdGUuIFBlcmNlbnQsIG9uZSBkZWNpbWFsIChhYm91dCA0LjElKS4KLSBPbi10aW1lIGZ1bGZpbG1lbnQgcmF0ZTogb3JkZXJzX212LCBPbiBUaW1lIEZ1bGZpbG1lbnQgUmF0ZS4gUGVyY2VudCwgdHdvIGRlY2ltYWxzIChhYm91dCA2Ny4zNyUpLiBUaGlzIGlzIHRoZSBzaGFyZSBvZiBhbGwgb3JkZXJzIGRlbGl2ZXJlZCBvbiB0aW1lLCBzbyBpdCByZWFkcyBvbiB0aGUgbG93IHNpZGUgYmVjYXVzZSB0aGUgbmV3ZXN0IG9yZGVycyBhcmUgbm90IGRlbGl2ZXJlZCB5ZXQuCgpDb21ibyBjaGFydCAoYmFycyBwbHVzIGEgbGluZSBvbiBhIHNlY29uZCBheGlzKSwgRGFpbHkgb3JkZXJzIGFuZCBjYW5jZWxsYXRpb24gcmF0ZToKLSBYID0gT3JkZXIgRGF0ZS4gQmFycyA9IE9yZGVycyBvbiB0aGUgbGVmdCBheGlzICh0aGUgdm9sdW1lKTsgTGluZSA9IENhbmNlbGxhdGlvbiBSYXRlIG9uIHRoZSByaWdodCBheGlzIChwZXJjZW50LCBheGlzIHN0YXJ0aW5nIGF0IDApLgotIFNvcnQgWCBieSBkYXRlIGFzY2VuZGluZy4gQ29sb3VyIHRoZSBiYXJzIGEgbmV1dHJhbCBibHVlIGFuZCB0aGUgbGluZSBhIHdhcm5pbmcgYW1iZXIgb3IgcmVkLCBzbyB0aGUgcmF0ZSByZWFkcyBhZ2FpbnN0IHRoZSB2b2x1bWUuCi0gUmVhZHM6IG9yZGVyIHZvbHVtZSBhbmQgdGhlIGNhbmNlbGxhdGlvbiByYXRlIGJvdGgganVtcCBvbiAyMjU3LTAzLTAzICgyMiw3NjEgb3JkZXJzIGFuZCA1LjklIGNhbmNlbGxlZCwgYWdhaW5zdCBhIGJhc2VsaW5lIG5lYXIgMy43JSkuIFRoZSBtaXNwcmljaW5nIHB1bGxlZCBpbiBhIGJ1eWluZyBzcGlrZSBhbmQgdGhlbiBhIHdhdmUgb2YgY2FuY2VsbGF0aW9ucyBvbiB0aGUgc2FtZSBkYXkuCgpMaW5lIGNoYXJ0IChjb2xvdXJlZCBieSBkZXBvdCksIENhbmNlbGxhdGlvbiByYXRlIGJ5IGRlcG90IG92ZXIgdGltZToKLSBYID0gT3JkZXIgRGF0ZSwgWSA9IENhbmNlbGxhdGlvbiBSYXRlIChwZXJjZW50KSwgY29sb3VyID0gRGVwb3QgKHNpeCBkZXBvdHMpLgotIFNvcnQgWCBieSBkYXRlOyBnaXZlIEFyZXMgRGVwb3QgYSBib2xkIGNvbG91ciBhbmQgbXV0ZSB0aGUgb3RoZXIgZml2ZSwgc28gdGhlIHN0b3J5IHBvcHMuCi0gUmVhZHM6IGZpdmUgZGVwb3RzIGhvbGQgYSBmbGF0IGxpbmUgYmV0d2VlbiBhYm91dCAyJSBhbmQgNSUgYWNyb3NzIHRoZSBwZXJpb2QsIGFuZCBvbmUsIEFyZXMgRGVwb3QsIHNwaWtlcyB0byBhYm91dCA5LjclIG9uIDIyNTctMDMtMDMgdGhlbiBkcm9wcyBiYWNrLiBUaGlzIGNoYXJ0IHBpbnMgdGhlIHByaWNpbmcgaW5jaWRlbnQgdG8gYSBzaW5nbGUgZGVwb3Qgb24gdGhlIGRheSByZXZlbnVlIHBlYWtlZC4KCgpQQUdFIDMsIG5hbWUgaXQgIkludmVudG9yeSIuICBNZXRyaWMgdmlldzogaW52ZW50b3J5X212LiAgU3Rvcnk6IGhvdyBtdWNoIHN0b2NrIG1vdmVzLCB3aGVyZSBpdCBydW5zIG91dCwgYW5kIGhvdyB0aGUgYnV5aW5nIHNwaWtlIGRyZXcgaXQgZG93bi4KQ291bnRlcnM6Ci0gU3RvY2sgbW92ZW1lbnRzOiBpbnZlbnRvcnlfbXYsIE1vdmVtZW50cy4gV2hvbGUgbnVtYmVyIChhYm91dCAyNzEsOTAwKS4KLSBTdG9ja291dHM6IGludmVudG9yeV9tdiwgU3RvY2tvdXQgRXZlbnRzLiBXaG9sZSBudW1iZXIgKDIxKSwgdGhlIGNvdW50IG9mIGRlcG90IGFuZCBwcm9kdWN0IHBhaXJzIHRoYXQgcmVhY2hlZCB6ZXJvIG9uIGhhbmQuCi0gVW5pdHMgc2hpcHBlZCBvdXQ6IGludmVudG9yeV9tdiwgVW5pdHMgT3V0LiBXaG9sZSBudW1iZXIgKGFib3V0IDEuNDZNKS4KCkFyZWEgY2hhcnQgKHN0YWNrZWQsIGNvbG91cmVkIGJ5IGRlcG90KSwgVW5pdHMgb3V0IGJ5IGRlcG90IG92ZXIgdGltZToKLSBYID0gTW92ZW1lbnQgRGF0ZSwgWSA9IFVuaXRzIE91dCwgY29sb3VyID0gRGVwb3QgKHNpeCBkZXBvdHMpLgotIFNvcnQgWCBieSBkYXRlIGFzY2VuZGluZzsgY29sb3VyIHNjYWxlIGNhdGVnb3JpY2FsLgotIFJlYWRzOiBlYWNoIGNvbG91ciBiYW5kIGlzIGEgZGVwb3QncyBzdG9jayBvdXRmbG93IHRoYXQgZGF5LiBFdmVyeSBkZXBvdCBkcmF3cyBkb3duIGhhcmRlc3QgYXJvdW5kIDIyNTctMDMtMDMgYXMgdGhlIGJ1eWluZyBzcGlrZSBsYW5kcywgd2l0aCBMdW5hIEh1YiBtb3ZpbmcgdGhlIG1vc3QgdW5pdHMsIHdoaWNoIGlzIHdoYXQgZW1wdGllcyB0aGUgc2hlbHZlcyBmb3IgdGhlIHN0b2Nrb3V0cy4KClRhYmxlLCBTdG9ja291dCB3YXRjaDoKLSByb3dzID0gRGVwb3Q7IGNvbHVtbnMgPSBNaW4gT24gSGFuZCwgU3RvY2tvdXQgRXZlbnRzLiBTb3J0IFN0b2Nrb3V0IEV2ZW50cyBkZXNjZW5kaW5nLgotIFJlYWRzOiBlYWNoIGRlcG90J3MgbG93ZXN0IHN0b2NrIGxldmVsIGFuZCBob3cgbWFueSB0aW1lcyBvbmUgb2YgaXRzIHByb2R1Y3RzIHJlYWNoZWQgemVybyBvbiBoYW5kLiBFdXJvcGEgT3V0cG9zdCwgYSBzbWFsbCBkaXN0YW50IGRlcG90LCBsZWFkcyB3aXRoIHR3ZWx2ZSBzdG9ja291dHM7IEx1bmEgSHViIGlzIHRoZSBvbmx5IGRlcG90IHRoYXQgbmV2ZXIgcmFuIG91dCAoaXRzIG1pbmltdW0gb24gaGFuZCBzdGF5cyBhdCBvbmUpLiBBcmVzIERlcG90J3Mgc2luZ2xlIHN0b2Nrb3V0IGlzIHRoZSBpbmNpZGVudCdzIFBSRC0wMDcgcGFydC4=";
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
# MAGIC An AI/BI dashboard on the `helios_semantic` metric views, in three clean, named pages, one per view: Sales Performance (`sales_mv`), Fulfilment (`orders_mv`) and Inventory (`inventory_mv`). Each page tells its story with the visuals that suit it: a revenue area, a gross-vs-net revenue line and product ranking on Sales, a dual-axis combo and a depot-coloured cancellation line on Fulfilment, and a stock-outflow area beside the Stockout watch on Inventory. You built every widget by picking a metric view's dimensions and measures, no SQL, so the dashboard reads the same governed definitions as Genie and the two can never disagree. Publish it when you are ready to share a clean read-only copy with the stakeholders.