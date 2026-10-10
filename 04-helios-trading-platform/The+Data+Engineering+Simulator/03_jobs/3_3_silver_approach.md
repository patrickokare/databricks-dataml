# Helios Trading Corporation: Our Approach in the Silver Layer

What we do to the data between Bronze and Silver, and how the way a feed arrives decides the way we write it. The sections below follow the lecture in the same order, so whatever is being talked through on screen has a section here with the detail in it.

---

## What Silver is for

Bronze is a raw copy of what arrived. Silver is the version the business reports from.

- Every column carries the data type it should have.
- Every table holds one row for each business key.
- The rows that break the contract are written into a quarantine table rather than dropped.

---

## Data types

Everything from the JSON and CSV feeds arrived in Bronze as a string, because those formats do not carry data types. Silver is where that gets fixed.

We name every column and cast it one by one, rather than selecting everything.

- A column the source starts sending is then simply not selected, so it cannot arrive in Silver untyped.
- A column the source stops sending fails the select outright, rather than turning into nulls that show up as a wrong number weeks later.

```python
.select(
    col("order_line_id").cast("string").alias("order_line_id"),
    col("quantity").cast("int").alias("quantity"),
    col("line_ts").cast("timestamp").alias("line_ts"),
    ...
)
```

---

## One row per business key

Two feeds send more than one row for the same order line or the same order, for two different reasons.

| Feed | Why more than one row arrives | What we want | Partition by | Most recent by |
|---|---|---|---|---|
| `order_lines` | the same line is submitted twice, so one sale reaches us as repeated `order_line_id`s | one of them | `order_line_id` | `line_ts` |
| `orders` | one row per change, so an order turns up again at each step from placed through to delivered | the state it is in now | `order_id` | `change_seq` |

The same pattern solves both. Partition by the key, order by the column that tells you which row is the most recent, keep row number one.

```python
window = Window.partitionBy("<key>").orderBy(col("<recency column>").desc())
latest = df.withColumn("_rn", row_number().over(window)).filter(col("_rn") == 1).drop("_rn")
```

The only thing that changes between the two is the recency column, and for `orders` that is `change_seq` rather than a timestamp, because changes can turn up out of order.

| Ordering by | What it gives you |
|---|---|
| the order the rows reached us | the last row received, which is not necessarily the last change that happened |
| `change_ts` | a timestamp stamped by the source system, which can be out of order too |
| `change_seq` | a number that counts up within an `order_id` at the source, so the highest is the latest change, whatever order the rows arrived in |

---

## Reducing the orders change feed

`orders` is a change data capture feed, so an order's changes reach us spread across batches as well as within one.

Worked example, order `ORD-000001` across the first two days.

| change_seq | status | op | in Silver |
|---|---|---|---|
| 1 | PLACED | INSERT | no |
| 2 | PAID | UPDATE | no |
| 3 | PICKED | UPDATE | no |
| 4 | SHIPPED | UPDATE | no |
| 5 | DELIVERED | UPDATE | yes |

Silver holds one row, the highest sequence number, so the order reads as DELIVERED.

Two more decisions on this feed.

- A latest change of `op = DELETE` means the order was cancelled and purged at the source, so we remove it from Silver too. Upsert the live orders first, then delete the purged ones.
- `op` is bookkeeping for the change feed, so it does not come into Silver. `change_seq` and `change_ts` do, because they tell us which version of the order we are looking at.

---

## Quarantine rules

A row that breaks the contract is moved into a quarantine table with the same schema as the clean one. It is never dropped.

| Table | A row is quarantined when |
|---|---|
| `silver_order_lines_quarantine` | `quantity` is missing, zero or negative, or `product_id` is missing |
| `silver_returns_quarantine` | the `order_line_id` does not exist in `silver_order_lines`, or `return_ts` is not later than the line's `line_ts` |

Drop a bad row and the sale it represented is missing from Gold with nothing left to trace it back to, so the shortfall turns up later as a total that nobody can account for. In a quarantine table the same row is something you can query, count, and take back to whoever owns the source.

For a sense of the scale.

| Order lines, batch one | Rows |
|---|---|
| landed in `bronze_order_lines` | 50,626 |
| kept in `silver_order_lines` | 49,151 |

Roughly a thousand of that difference is duplicate submissions and the rest broke the contract and is sitting in quarantine.

A return pointing at `OL-0-9999990`, an order line that does not exist, has no price and no product behind it, so it cannot be valued and it joins to nothing. It goes to quarantine rather than into the numbers.

---

## How each feed is written

The way a feed is written is decided by the way it arrives.

| Feed | How it arrives | How we write it |
|---|---|---|
| order_lines | new rows only, with duplicates and a few bad rows | deduplicate, then upsert on `order_line_id` |
| orders | one row per change to an order | keep the latest change per `order_id`, upsert, remove the purged ones |
| returns | new rows only, days after the sale they relate to | check against the line, then upsert on `return_id` |
| inventory | new stock movements only | upsert on `warehouse_id`, `product_id` and `movement_ts` |
| products, categories, suppliers, warehouses | the whole list, every time | overwrite |

Upsert the feeds that send changes. Overwrite the feeds that send the whole picture.

---

## Overwriting the full snapshots

Products, categories, suppliers and warehouses arrive as the full current list every time. We overwrite them rather than merge them, because a merge only ever adds and updates: retire a product at the source and a merge leaves it in our table forever, because nothing in the feed says it went. Rebuilding the table from the latest snapshot drops it, and repairs anything an earlier run got wrong at the same time.

The cost is that we keep no history of these tables, which we can afford here because the lists are small and slow moving. Where history has to be kept, the table is built as a slowly changing dimension instead.

---

## The upsert helper

Almost every table here does the same two things. Make sure the target exists, then merge the new rows in on a key.

Writing that once, as a helper, keeps the merge logic in a single place. When it needs fixing, it gets fixed everywhere at the same time.

```python
def upsert(source, table, keys):
    ...
    (DeltaTable.forName(spark, table).alias("t")
        .merge(source.alias("s"), condition)
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
```

Two things make it reusable.

- It creates the table from the source's own schema if it is not there yet, so a first run works the same as every other run.
- It takes either a single key or a list of them, which is what `silver_inventory` needs, because a stock movement is only unique on the depot, the product and the timestamp together.

Merging on a key is also what makes the layer safe to re-run. A row that is already there is updated in place rather than added a second time.

---

## Streaming order_lines from Bronze

`order_lines` is the biggest feed in the pipeline, so Silver reads it out of Bronze as a stream with its own checkpoint. Each run picks up only the rows that landed since the last one, the same incremental bookkeeping Auto Loader gives us in Bronze, one layer up.

That leaves us needing to merge those rows into Silver, and a stream write cannot merge.

---

## Why a stream cannot merge on its own

Two separate things stop it.

The first is that when you write a stream you do not tell Spark which rows to write. You describe the query, you pick an output mode, and the engine decides what to emit on each trigger.

| Output mode | What the engine emits | Delta sink |
|---|---|---|
| `append` | rows that will not change again | accepted |
| `complete` | the whole result, rewritten on every trigger | accepted |
| `update` | only the rows that changed | not accepted |

All three say what to emit. None of them looks at the target table first, and looking first is what a merge is: check whether this `order_line_id` is already in Silver, update that row if it is, insert it if it is not.

The second is that `DeltaTable.merge()` is a batch API. It joins the incoming rows against the target, so it needs a set of rows with an end to it. A streaming DataFrame has no end, so there is nothing to join, and the call is rejected with `Queries with streaming sources must be executed with writeStream.start()`.

---

## What foreachBatch does

`foreachBatch` gets round both. Instead of giving `writeStream` a sink, we give it a function, and Spark calls that function once per micro batch with two arguments: the rows in that batch as a DataFrame, and an increasing batch id.

```python
def to_silver_<table>(micro_batch, _batch_id):
    # micro_batch is a static DataFrame. Ordinary batch code from here on.
    ...


(
    typed_stream.writeStream
    .foreachBatch(to_silver_<table>)
    .option("checkpointLocation", f"{chk}/<table>")
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)
```

The rows in `micro_batch` are known and finite, so inside the function we are back in ordinary batch code. `merge` works there, and so do window functions, joins to other tables, `.count()`, and writing to more than one table from the same batch. `order_lines` needs that last one: the clean rows and the quarantined rows come out of one pass over the batch and go to two different tables.

The dedup window runs inside the function, so it only ever sees the rows in that one micro batch. Two copies of an `order_line_id` that arrive in different batches are not caught by it. They are caught by the merge instead, which finds the key already in the table and updates that row rather than adding a second one.

Two things to know before you write one.

### Every action recomputes the batch

The function takes two actions on each micro batch, one merge into the clean table and one into quarantine, so the read, the typing and the dedup window all run twice for that batch. Persisting the DataFrame is the usual way out of that, and serverless does not support it, so we take the two passes instead. At this size they cost very little.

### Running the same batch twice

`foreachBatch` is at-least-once. If the job dies after the merge has committed but before the checkpoint has been updated, that batch runs again on the next start.

Merging the same rows on the same key twice leaves the table in exactly the state the first merge left it in, so the repeat costs a little time and changes nothing. An append would have written every row a second time.

---

## Silver tables and write modes

| Table | Key | Written by |
|---|---|---|
| `silver_order_lines` | `order_line_id` | stream, then upsert |
| `silver_order_lines_quarantine` | `order_line_id` | stream, then upsert |
| `silver_orders` | `order_id` | upsert, then delete purged |
| `silver_returns` | `return_id` | upsert |
| `silver_returns_quarantine` | `return_id` | upsert |
| `silver_inventory` | `warehouse_id`, `product_id`, `movement_ts` | upsert |
| `silver_products` | `product_id` | overwrite |
| `silver_categories` | `category_id` | overwrite |
| `silver_suppliers` | `supplier_id` | overwrite |
| `silver_warehouses` | `warehouse_id` | overwrite |

`price_list` and `customers` are not in this list. Both carry history that has to be kept, so they are built as slowly changing dimensions of their own.

---

## References

- [Upsert into a Delta Lake table using merge](https://docs.databricks.com/aws/en/delta/merge)
- [Use foreachBatch to write to arbitrary data sinks](https://docs.databricks.com/aws/en/structured-streaming/foreach)
- [Delta Lake table streaming reads and writes](https://docs.databricks.com/aws/en/structured-streaming/delta-lake)
- [Window functions](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-window-functions)
