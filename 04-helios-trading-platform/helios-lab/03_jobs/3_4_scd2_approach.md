# Helios Trading Corporation: How We Handle Slowly Changing Dimensions

Two things in our data change over time and both have to be readable as they were, not as they are now. What a product sold for, and what tier a customer was on. This is how we keep that history, and why the two of them are built from different starting points.

---

## Why we keep history

`PRD-001` sold for 269.99 CREDITS on 2257-03-01. On 2257-03-03 the price went up to 288.89.

If we hold one row per product and overwrite it when the price moves, then every sale we ever made is suddenly valued at 288.89. Last month's revenue changes overnight, and it changes again the next time somebody edits a price. Nobody can close a set of books on a table that does that.

The same problem with a different shape on customers. A customer on the DRIFTER tier gets promoted to TRADE. Overwrite the tier and every sale they ever made is reported as a TRADE sale, including the ones made when they were paying DRIFTER rates.

So both of these are kept as Slowly Changing Dimensions, Type 2. Instead of updating a row we add one, and we date each version, so a sale can always be read against the version that was in force on the day.

For contrast, our reference lists (products, categories, suppliers and depots) are handled Type 1. They get overwritten and no history is kept, because nobody needs to know what a product was called last year.

---

## What a Type 2 table looks like

Every version is a row. Every row carries the range of dates it was true for, a flag for the open one, and a surrogate key of its own.

`silver_price_scd` for `PRD-001`.

| price_sk | product_id | unit_price | unit_cost | effective_from | effective_to | is_current |
|---|---|---|---|---|---|---|
| PRD-001_22570301 | PRD-001 | 269.99 | 183.59 | 2257-03-01 | 2257-03-03 | false |
| PRD-001_22570303 | PRD-001 | 288.89 | 192.77 | 2257-03-03 | null | true |

Four rules hold on every SCD Type 2 table we build.

- One version's `effective_to` is exactly the next version's `effective_from`. The ranges meet, so there are no gaps and no overlaps.
- A date belongs to the version where `effective_from` is on or before it and `effective_to` is after it, or where `effective_to` is null.
- The open version has `effective_to` set to null, not to a far future date like 9999-12-31. Both conventions are used in the wild. Ours is null, and `is_current` is derived from it, so the two can never disagree with each other.
- Exactly one row per key has `is_current` true.

The surrogate key is the natural key plus the version's start date, so `PRD-001_22570301`. It is readable, it is unique per version, and it comes out the same every time we rebuild. That last property is what the merge depends on. Because a given version always produces the same key, a second run finds its row already there and updates it, instead of inserting a duplicate version alongside it.

Money is `DECIMAL(12,2)`, never a float. Floats do not hold decimal fractions exactly, and the errors accumulate through a sum. That is fine for a temperature reading and unacceptable in a revenue figure.

---

## The two dimensions and their feeds

We build two SCD Type 2 dimensions, the price dimension and the customer dimension. They finish in the same shape, but they start from very different places, because the two source feeds tell us different amounts about their own history.

| | price_list | customers |
|---|---|---|
| How it arrives | an effective dated extract, the whole price history re-sent every day | a full snapshot of every customer, every day |
| Does it tell us about versions | yes, each row already is one version with its own start date | no, there is nothing in the file that says anything changed |
| So the first job is | reduce the repeated history to the distinct versions | compare each day against the day before and find the changes |
| What starts a version | a new `effective_from` in the feed | the tier being different from yesterday's snapshot |

The difference is the whole point. A feed either knows its own history or it does not, and you build with whatever the source system gives you. The next two sections build one dimension each.

---

## The price dimension

**The situation.** `price_list` is a CSV extract of the full price list, and it is effective dated, so each row already carries the date its price started. The whole thing is re-sent every day, so three days of landings hold three overlapping copies of the same history.

What lands, simplified, for one product.

| batch | product_id | unit_price | effective_from |
|---|---|---|---|
| 1 | PRD-001 | 269.99 | 2257-03-01 |
| 2 | PRD-001 | 269.99 | 2257-03-01 |
| 3 | PRD-001 | 269.99 | 2257-03-01 |
| 3 | PRD-001 | 288.89 | 2257-03-03 |

That is two real versions of `PRD-001`, landed four times between them.

**The problem.** The repeats have to be collapsed down to the distinct versions. And the feed's own `effective_to` and `is_current` columns cannot be trusted across files, because each one only describes the single file it was written in.

**Our approach.**

- Reduce to one row per product and start date, which throws the repeats away.

  ```python
  .dropDuplicates(["product_id", "effective_from"])
  ```

- Close each version off with the start date of the version after it.

  ```python
  by_product = Window.partitionBy("product_id").orderBy("effective_from")
  .withColumn("effective_to", lead("effective_from").over(by_product))
  ```

- Mark the version with nothing after it as the current one, and build the `price_sk` from the product id and the version's start date.

We recompute `effective_to` and `is_current` ourselves rather than carry the feed's own values, because each file's copy only describes the source system's view at the moment it was written and we are stitching several files together. Working them out from the versions we actually hold keeps the table internally consistent, even when a file is re-sent or arrives out of order.

---

## The customer dimension

**The situation.** `customers` gives us no help at all. The same 300 customers arrive as a Parquet snapshot every day, and a promotion looks exactly like every other row.

What lands for `CUST-0033`.

| snapshot_date | customer_id | tier |
|---|---|---|
| 2257-03-01 | CUST-0033 | DRIFTER |
| 2257-03-02 | CUST-0033 | DRIFTER |
| 2257-03-03 | CUST-0033 | TRADE |

**The problem.** Nothing in that file says a promotion happened. The only way to see the change is to line the days up for each customer and look for the point where the tier is different from the day before.

**Our approach.**

- Keep one row per customer per day.

  ```python
  .dropDuplicates(["customer_id", "snapshot_date"])
  ```

- Look back at the previous snapshot for that customer and keep only the rows where the tier changed, plus the very first snapshot for each customer.

  ```python
  by_customer = Window.partitionBy("customer_id").orderBy("snapshot_date")
  .withColumn("prev_tier", lag("tier").over(by_customer))
  .filter(col("prev_tier").isNull() | (col("tier") != col("prev_tier")))
  ```

  Those surviving rows are the version starts. Two rows for `CUST-0033`, and one row for the roughly 90 percent of customers who never move.

- From here it is the same as the price build. `lead` closes each version off with the next one's start date, the open version is current, and the snapshot date becomes `effective_from`.

The filter has to run before the `lead`. Filter first and `lead` looks at the next *version*, which is what we want. Do it the other way round and `lead` looks at the next *day*, so every closed version comes out one day wide and the history is wrong.

The result.

| customer_sk | tier | effective_from | effective_to | is_current |
|---|---|---|---|---|
| CUST-0033_22570301 | DRIFTER | 2257-03-01 | 2257-03-03 | false |
| CUST-0033_22570303 | TRADE | 2257-03-03 | null | true |

This table carries the full customer record in every version, the name, the type, the home depot and the signup date, not just the tier, which makes it the customer dimension itself rather than a side table of tier changes. That is why there is no separate current customer table anywhere in Silver. Only the tier creates a version: if a customer's name were corrected, that fix should apply to the existing row rather than open a new version.

---

## How both builds finish

We found the versions in two different ways, but from here both builds are identical. A window builds the full version history, with each version's effective date range, and a single Delta merge on the surrogate key writes it to the table, updating the versions already there and inserting the new ones.

```python
(DeltaTable.forName(spark, table).alias("target")
    .merge(versions.alias("source"), "target.<sk> = source.<sk>")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())
```

Three things fall out of building it this way.

**It is right when several days land at once.** The window is computed over every version we hold, not over the newest file. So if three batches land in one run, which is exactly what happens when somebody rebuilds from scratch, the history comes out the same as if they had arrived one a day.

**It is safe to run twice.** The surrogate key is derived from the version's start date, so the same version always produces the same key. Running the build again updates the rows that are already there and inserts nothing new.

**Closing a version is just an update.** When a new price arrives, the previous version's `effective_to` changes from null to the new date and its `is_current` flips to false. The merge picks that up through `whenMatchedUpdateAll`, because that row's key has not changed. There is no separate close step to write and no chance of the two steps disagreeing.

What we do not do is loop over the dates and replay them one at a time, or pull the versions back to the driver with `collect`. The window does the shaping in one pass over the data.

---

## Point in time valuation

A sale is valued by joining the order line to the price version whose range covers the line's timestamp, rather than to whatever the price happens to be now.

The Ares incident shows what that buys. `PRD-007` was 7,633.99, dropped to 3,435.30 on 2257-03-03 against a cost of 4,198.69, and was corrected to 7,634.00 the next day.

| price_sk | unit_price | effective_from | effective_to | is_current |
|---|---|---|---|---|
| PRD-007_22570301 | 7633.99 | 2257-03-01 | 2257-03-03 | false |
| PRD-007_22570303 | 3435.30 | 2257-03-03 | 2257-03-04 | false |
| PRD-007_22570304 | 7634.00 | 2257-03-04 | null | true |

Three versions, and the middle one is a day of sales made below cost. Value those lines at today's price and the loss disappears completely. Value them at the version in force and it is sitting there in the margin, on one depot, on one day.

---

## The two dimensions and their keys

| | silver_price_scd | silver_customer_scd |
|---|---|---|
| Natural key | `product_id` | `customer_id` |
| Surrogate key | `price_sk`, `<product_id>_<YYYYMMDD>` | `customer_sk`, `<customer_id>_<YYYYMMDD>` |
| Source | `bronze_price_list`, effective dated CSV | `bronze_customers`, daily Parquet snapshot |
| Version starts when | the feed carries a new `effective_from` | `tier` differs from the previous snapshot |
| Window functions used | `lead` | `lag` then `lead` |
| Tracked attribute | the price and cost carried on the version | `tier` |
| Carries | product price, cost, currency and supplier | the full customer record |
| Written by | one merge on `price_sk` | one merge on `customer_sk` |

---

## References

- [Upsert into a Delta Lake table using merge](https://docs.databricks.com/aws/en/delta/merge)
- [Window functions](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-window-functions)
- [DECIMAL type](https://docs.databricks.com/aws/en/sql/language-manual/data-types/decimal-type)
- [Delta Lake table streaming reads and writes](https://docs.databricks.com/aws/en/structured-streaming/delta-lake)
