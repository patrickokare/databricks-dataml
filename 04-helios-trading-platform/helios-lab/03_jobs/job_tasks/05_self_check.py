# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline task: Self check (quality gate)
# MAGIC
# MAGIC The final Job task. It runs unit test style assertions against the built tables and fails the run if any do not
# MAGIC hold, so a broken pipeline never reports success. Lab 3.11 also validates the run from the outside with the SDK.

# COMMAND ----------

# MAGIC %run ../../00_setup/bootstrap_helpers

# COMMAND ----------

from pyspark.sql.functions import col, lead
from pyspark.sql import Window

catalog = helios_identity(verbose=False)
bronze = f"{catalog}.helios_bronze"
silver = f"{catalog}.helios_silver"
gold = f"{catalog}.helios_gold"

# Everything Gold needs exists (fact_returns is batch 2+, checked separately).
for table in ["dim_product", "dim_customer", "dim_supplier", "dim_warehouse", "dim_category",
              "dim_date", "fact_order_lines", "fact_inventory"]:
    check_true(f"{gold}.{table} exists", spark.catalog.tableExists(f"{gold}.{table}"), hard=True)

# Silver order_lines is deduplicated on the business key.
lines = spark.table(f"{silver}.silver_order_lines")
check("silver_order_lines has no duplicate order_line_id", lines.count(),
      lines.select("order_line_id").distinct().count(), hard=True)

# Price SCD2: one current per product, contiguous non overlapping ranges.
price = spark.table(f"{silver}.silver_price_scd")
check("price SCD2 has exactly one current row per product", 0,
      price.filter("is_current").groupBy("product_id").count().filter("count <> 1").count(), hard=True)
pr = price.withColumn("next_from",
                      lead("effective_from").over(Window.partitionBy("product_id").orderBy("effective_from")))
check("price SCD2 effective ranges are contiguous and non overlapping", 0,
      pr.filter("next_from IS NOT NULL AND (effective_to IS NULL OR effective_to <> next_from)").count(), hard=True)

# Tier SCD2: one current per customer.
customer_scd = spark.table(f"{silver}.silver_customer_scd")
check("customer SCD2 has exactly one current row per customer", 0,
      customer_scd.filter("is_current").groupBy("customer_id").count().filter("count <> 1").count(), hard=True)
check("customer_sk is unique in the customer SCD2", customer_scd.count(),
      customer_scd.select("customer_sk").distinct().count(), hard=True)

# fact_order_lines: one per line, matches Silver, every line priced by the point in time join.
fact = spark.table(f"{gold}.fact_order_lines")
check("fact_order_lines grain is one row per line", fact.count(),
      fact.select("order_line_id").distinct().count(), hard=True)
check("fact_order_lines matches silver_order_lines", lines.count(), fact.count(), hard=True)
check("every order line is priced by the point in time join", 0,
      fact.filter("price_sk IS NULL").count(), hard=True)

# Referential integrity: every fact order_id appears in the orders CDC feed. It need not survive in
# silver_orders, because a subset of cancellations are deliberately purged there (their latest CDC op is
# DELETE) while their lines remain in the immutable order_lines ledger, so resolve against the full feed.
orders_feed = spark.table(f"{bronze}.bronze_orders").select("order_id").distinct()
check("every fact order_id appears in the orders CDC feed", 0,
      fact.select("order_id").distinct().join(orders_feed, "order_id", "left_anti").count(), hard=True)
check("fact_order_lines order_status is null or a valid lifecycle status", 0,
      fact.filter("order_status IS NOT NULL AND order_status NOT IN "
                  "('PLACED','PAID','PICKED','SHIPPED','DELIVERED','BACKORDERED','CANCELLED','RETURNED')").count(), hard=True)

# fact_inventory carries a date on every movement.
inv = spark.table(f"{gold}.fact_inventory")
check("every inventory movement has a date_key", 0, inv.filter("date_key IS NULL").count(), hard=True)

# fact_returns (from batch 2): every return refers to a real line and carries a refund.
if spark.catalog.tableExists(f"{gold}.fact_returns"):
    fr = spark.table(f"{gold}.fact_returns")
    check("every return refers to a real order line", 0,
          fr.select("order_line_id").distinct().join(lines.select("order_line_id"), "order_line_id", "left_anti").count(),
          hard=True)
    check("every return has a refund amount", 0, fr.filter("refund_amount IS NULL").count(), hard=True)

print(f"Gold verified for {catalog}")