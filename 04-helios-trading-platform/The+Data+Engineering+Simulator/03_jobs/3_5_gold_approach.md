# Helios Trading Corporation: Our Approach in the Gold Layer

What we do to the data between Silver and Gold, why the dimensions are overwritten while the facts are upserted, and how a sale gets valued at the price that was in force when it was placed. The sections below follow the lecture in the same order, so whatever is being talked through on screen has a section here with the detail in it.

---

## What Gold is for

Silver is correct. Gold is shaped for the questions the business asks.

- One star schema in `helios_gold`, three facts and six dimensions, and every downstream consumer reads it.
- Nothing is cleaned or repaired here. A row that reaches Gold was already fixed in Silver, or it never left quarantine.
- No aggregates. Gold holds the events at their own grain, and the reusable metrics are defined once in the semantic layer.

---

## The star schema

A fact table records events. One row per thing that happened, carrying the numbers we measure, and the keys of everything involved in it. A dimension table describes those things, so it carries the attributes we group and filter by. A question is answered by joining a fact to the dimensions it points at.

```
        dim_product     dim_customer (SCD Type 2)
                  \        /
   dim_supplier -- fact_order_lines -- dim_date
                  /        \
      dim_warehouse       dim_category
                           (via dim_product)

   fact_returns and fact_inventory point at the same dimensions.
```

`order_id` and `order_line_id` sit on the fact rather than in a dimension of their own. There is nothing to describe about an order id beyond the id itself, so it rides along on the fact as a degenerate dimension, still available to group by and to trace a row back to the source.

---

## The dimensions

| Dimension | Built from | Describes |
|---|---|---|
| `dim_product` | `silver_products` | the part, its SKU, category, supplier, mass and hazard class |
| `dim_customer` | `silver_customer_scd` | the customer and their tier, one row per version |
| `dim_supplier` | `silver_suppliers` | the supplier and their home region |
| `dim_warehouse` | `silver_warehouses` | the depot, the body it orbits, its region and uplink reliability |
| `dim_category` | `silver_categories` | the category and the department it rolls up to |
| `dim_date` | generated | one row per calendar day |

Two entries in that list need spelling out.

`dim_customer` is the customer SCD Type 2, carried through from Silver with its versions intact, so it holds each customer's tier history rather than only their tier today. The facts carry `customer_id`, not `customer_sk`, so the join decides which version you get: match on `is_current` for the customer as they are now, or match the version whose effective range covers the fact's date for the customer as they were on the day.

There is no price dimension in Gold. `silver_price_scd` is already conformed and versioned in Silver, and the facts price against it directly, so copying it up would leave two tables to keep in step for no gain.

---

## dim_date

Every fact carries a `date_key`, an integer in `YYYYMMDD` form derived from the row's own timestamp, so `2257-03-01` becomes `22570301`. `dim_date` holds one row per day across the range the data covers, keyed the same way.

```python
dates = (
    spark.read.table(f"{silver}.silver_order_lines").select(to_date("line_ts").alias("d"))
    .union(spark.read.table(f"{silver}.silver_inventory").select(to_date("movement_ts").alias("d")))
)
span = dates.select(min_("d").alias("from_date"), max_("d").alias("to_date"))
```

The calendar is generated from the span of the line and movement dates, then expanded a day at a time, so it always covers the data and never runs short of it. Each row carries the year, the month, the day, the day name and a weekend flag.

Without it, every date question is answered by pulling the parts out of a timestamp in the query, and two analysts asking the same question two different ways get two different answers. With it, the calendar is a table everyone joins to.

---

## Why the dimensions are overwritten and the facts are upserted

| Table | Written by | Why |
|---|---|---|
| the six dimensions | overwrite | each one is a complete picture, small, and derived straight from Silver |
| the three facts | upsert on the business key | each one is a large, growing event log, so a run should only touch the rows in front of it |

Overwriting a dimension is a self healing write. Whatever the last run produced is thrown away and the table is rebuilt from Silver, so a dimension can never drift away from its source, and a row deleted upstream disappears here rather than sitting in the table forever. That holds for the SCD Type 2 customer dimension too, because Silver holds the whole version history, so rebuilding it reproduces every version rather than losing the old ones.

Rebuilding a fact is a different proposition. `fact_order_lines` holds a row for every order line Helios has ever sold, so the cost of a full rebuild grows with the business. The facts are upserted on their business key instead, which touches only the rows in the batch and leaves the rest untouched. The same key always identifies the same row, so a re-run updates in place and inserts nothing new.

```python
def upsert(source, table, keys):
    ...
    (DeltaTable.forName(spark, table).alias("t")
        .merge(source.alias("s"), condition)
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
```

It is the same helper the Silver layer uses, and it takes either a single key or several columns together, which `fact_inventory` needs.

---

## Point in time pricing

`order_lines` arrives with no money on it. The sale and the price live on different feeds, so the price has to be joined on, and which version of the price we join decides whether the numbers are right.

Every line is matched to the price version whose effective range covers the moment the line was placed.

```python
lines.join(
    prices,
    (col("l.product_id") == col("p.product_id"))
    & (to_date(col("l.line_ts")) >= col("p.effective_from"))
    & (col("p.effective_to").isNull() | (to_date(col("l.line_ts")) < col("p.effective_to"))),
    "left")
```

Three properties make that join safe.

- It cannot fan out. The SCD Type 2 build leaves the ranges contiguous and non overlapping per product, so exactly one version can cover any given date. The fact stays at one row per `order_line_id`.
- Every line resolves. `price_list` carries every product from the base date, so there is no line without a price behind it.
- It is not affected by a later price change. The version in force on the day is a fact about that day, so a line valued once keeps the same value forever.

From the matched version the fact takes `unit_price`, `unit_cost`, `supplier_id` and `price_sk`, and computes the two measures.

| Measure | Definition |
|---|---|
| `gross_amount` | `quantity` times `unit_price` |
| `margin` | `gross_amount` minus `quantity` times `unit_cost` |

Worked example. `OL-1-000001` sold two of `PRD-017` on `2257-03-01`.

| order_line_id | product_id | date_key | price_sk | unit_price | quantity | gross_amount | margin |
|---|---|---|---|---|---|---|---|
| `OL-1-000001` | `PRD-017` | `22570301` | `PRD-017_22570301` | `10184.99` | `2` | `20369.98` | `8962.80` |

`price_sk` is kept on the fact, so any figure can be traced back to the exact price version it was calculated from.

---

## `fact_order_lines`

Grain: one row per order line.

| Column | Type | Notes |
|---|---|---|
| order_line_id | STRING | the grain and the merge key |
| order_id | STRING | degenerate dimension |
| product_id, customer_id, warehouse_id | STRING | foreign keys to the dimensions |
| supplier_id | STRING | from the priced version, so it is the supplier who sold it at that price |
| date_key | INT | from `line_ts` |
| price_sk | STRING | the price version used |
| quantity | INT | measure |
| unit_price, unit_cost | DECIMAL(12,2) | the point in time price and cost per unit |
| gross_amount, margin | DECIMAL(14,2) | measures |
| is_returned | BOOLEAN | true when the line appears in `silver_returns` |
| order_status | STRING | the order's current lifecycle status, or null |

This is a bookings grain. Every placed line has a row, including lines on orders that are still in flight and lines on orders that were later cancelled, so revenue read straight off this table is gross bookings.

`order_status` is what makes that workable. It is left joined from `silver_orders`, which is already reduced to one row per order, so the join cannot fan out, and it lets a reader net out cancellations by excluding `order_status = 'CANCELLED'`. It is null when the order has no current record in Silver, either because its change has not arrived yet or because it was a purged cancellation.

A return never removes a line and never reduces its value. `is_returned` flags it, and the refund is recorded separately in `fact_returns`, so a return can be reported without a sale quietly disappearing from last month's revenue.

---

## `fact_returns`

Grain: one row per return.

| Column | Type | Notes |
|---|---|---|
| return_id | STRING | the grain and the merge key |
| order_line_id | STRING | the returned line, a degenerate dimension |
| product_id, customer_id, warehouse_id | STRING | carried from the original line |
| date_key | INT | from `return_ts` |
| quantity_returned | INT | measure |
| refund_amount | DECIMAL(14,2) | `quantity_returned` times the original paid `unit_price` |

The refund is valued by joining the return to its original line, then point in time joining that line to the price dimension on the line's own `line_ts`. The customer is refunded what they paid, not what the part costs today.

The table is empty until returns start arriving, because a return always follows the sale it relates to by some days.

---

## `fact_inventory`

Grain: one row per stock movement, keyed on `warehouse_id`, `product_id` and `movement_ts` together.

| Column | Type | Notes |
|---|---|---|
| warehouse_id, product_id | STRING | foreign keys to the dimensions |
| movement_ts | TIMESTAMP | when the movement happened |
| date_key | INT | from `movement_ts` |
| delta | INT | measure, the signed change |
| on_hand | INT | measure, the running balance after the movement |

Keeping the movement grain rather than a daily balance means stock can be charted over time for a depot and product, and a stockout can be read as the moment `on_hand` reached zero rather than inferred from a day's totals.

---

## Gold tables and write modes

| Table | Key | Written by |
|---|---|---|
| `dim_product` | `product_id` | overwrite |
| `dim_customer` | `customer_sk` | overwrite |
| `dim_supplier` | `supplier_id` | overwrite |
| `dim_warehouse` | `warehouse_id` | overwrite |
| `dim_category` | `category_id` | overwrite |
| `dim_date` | `date_key` | overwrite |
| `fact_order_lines` | `order_line_id` | upsert |
| `fact_returns` | `return_id` | upsert |
| `fact_inventory` | `warehouse_id`, `product_id`, `movement_ts` | upsert |

---

## Where the metrics live

Gold holds no totals. Revenue, units sold, average order value, gross margin, return rate, cancellation rate and on time fulfilment are defined once as Metric Views over these tables in the semantic layer, and Genie and the dashboards read the same definitions.

Sum `gross_amount` into a Gold table and the definition of revenue is fixed at the moment that table was built. Anyone who wants it net of cancellations, or by department rather than category, needs a new table. Leaving the facts at their own grain means one definition can be changed in one place and every consumer sees the change.

---

## References

- [Data modeling](https://docs.databricks.com/aws/en/transform/data-modeling)
- [Upsert into a Delta Lake table using merge](https://docs.databricks.com/aws/en/delta/merge)
- [Point-in-time feature joins](https://docs.databricks.com/aws/en/machine-learning/feature-store/time-series)
- [Range join optimization](https://docs.databricks.com/aws/en/optimizations/range-join)
- [DECIMAL type](https://docs.databricks.com/aws/en/sql/language-manual/data-types/decimal-type)
