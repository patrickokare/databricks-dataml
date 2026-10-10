# Helios Trading Corporation: Ingestion into Bronze with Auto Loader

How our source files arrive, and how we load them into Bronze. The sections below follow the lecture in the same order, so whatever is being talked through on screen has a section here with the detail in it.

---

## The source files

Another system writes the files. We pick them up from the landing Volume. Each feed has a folder of its own, and every drop creates a new folder inside it.

```
/Volumes/labs_<userid>/helios_raw/helios_landing/
  order_lines/
    batch_1/part-00000-....json
    batch_2/part-00000-....json
  orders/
    batch_1/part-00000-....json
  price_list/
    batch_1/part-00000-....csv
  customers/
    snapshot_2257-03-01/part-00000-....parquet
  products/
    batch_1/part-00000-....parquet
```

There are three kinds of feed.

- **New rows only.** `order_lines`, `orders`, `returns` and `inventory`. Each drop is that day's new records and nothing else, so the folders stack up day after day.
- **The whole list every time.** `customers` sends all 300 records daily whether anything changed or not, and `price_list` sends every price we have ever had, current and expired together. These are snapshots, and the customer folders are named after the day they cover rather than the batch.
- **Only when something changes.** `products`, `categories`, `suppliers` and `warehouses`. Most days there is no file at all, then one appears because a supplier was added.

| Feed | Format | Folder | Arrives |
|---|---|---|---|
| order_lines | JSON | `order_lines/batch_<n>/` | daily |
| orders | JSON | `orders/batch_<n>/` | daily |
| returns | JSON | `returns/batch_<n>/` | daily, from day two |
| inventory | JSON | `inventory/batch_<n>/` | daily |
| price_list | CSV with header | `price_list/batch_<n>/` | daily |
| customers | Parquet | `customers/snapshot_<date>/` | daily |
| products | Parquet | `products/batch_<n>/` | when it changes |
| categories | Parquet | `categories/batch_<n>/` | when it changes |
| suppliers | JSON | `suppliers/batch_<n>/` | when it changes |
| warehouses | JSON | `warehouses/batch_<n>/` | when it changes |

Ingestion treats all of them the same way. A file is a file and we append what is in it. Whether a feed sends new rows or a full snapshot decides whether Silver merges it or replaces the table, which is a Silver decision and not an ingestion one.

---

## What we're building in Bronze

One table per feed, named `bronze_<feed>`, in the `helios_bronze` schema.

Rows land exactly as they arrived, with three columns added on top. No deduplication, no typing, no filtering, no business rules. Those all happen once in Silver, where they are visible and testable.

Keeping the layer this thin buys two things. A run only ever appends the files that turned up since the last one, so its cost is set by a day's arrivals and not by the size of the table, which is the same on day one thousand as on day one. And because nothing here has been changed from what the source sent, every layer above can be dropped and rebuilt from Bronze without going back to the source system.

---

## Why Auto Loader

Files arrive in a new dated folder every day, so we could handle this ourselves. Read today's folder, load it, move on.

That holds until one of three ordinary things happens.

- A file turns up late and lands in an older folder, so reading today's folder misses it.
- A run fails halfway through, and now nobody knows which files made it into the table and which did not.
- A batch gets re-sent, and reading the folder again loads all of it a second time.

Each of those pushes us towards keeping our own list of every file we have already loaded, checking the folder against that list on every run, and repairing the list when a run dies. It is not much code, and it is a bad piece of code to get wrong, because the two ways it fails are loading a file twice and never loading it at all.

Auto Loader is that list, done properly. We point it at the feed folder and it only processes new files, wherever they landed and whenever they arrived.

---

## The checkpoint

The record Auto Loader keeps is called the checkpoint, and it is a list of the files it has already processed. It is written as part of the same commit that writes the rows, so a file is only ever marked as done if its rows actually landed.

Auto Loader needs somewhere to keep that list, and somewhere to keep the schema it works out for the feed. Both live in a Volume we create for the purpose, with a folder per feed.

```
/Volumes/labs_<userid>/helios_bronze/checkpoints/
  order_lines/
    schema/      cloudFiles.schemaLocation, the inferred schema (written under _schemas)
    chk/         checkpointLocation, the record of files already processed
  orders/
    schema/
    chk/
  price_list/
    schema/
    chk/
```

One folder each, because a schema location holds exactly one schema and a checkpoint holds exactly one list of files. Point two feeds at the same path and the second one is parsed against the first one's columns and skips files it has never read.

Delete the `chk` folder and the next run treats every file as new, which reloads the whole feed and duplicates Bronze. Delete the `schema` folder and Auto Loader infers the schema again from scratch. Keep both and the load stays incremental.

One consequence of tracking files rather than rows. If the source re-sends yesterday's data under a **new file name**, Auto Loader has no way to tell it apart from genuinely new data, and it ingests the lot a second time. Nothing inspects the contents of a file against what is already in the table. That is why the bootstrap in the labs skips a batch that has already landed instead of writing it again.

---

## Syntax

Every feed is loaded the same way. Read the feed folder with Auto Loader, add the metadata columns, write to a Delta table.

```python
from pyspark.sql.functions import col, current_timestamp

chk  = f"/Volumes/{catalog}/helios_bronze/checkpoints"
root = f"/Volumes/{catalog}/helios_raw/helios_landing"

stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "<format>")
    .option("cloudFiles.schemaLocation", f"{chk}/<feed>/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("rescuedDataColumn", "_rescued_data")
    .load(f"{root}/<feed>")
    .withColumn("_ingest_ts", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
)

query = (
    stream.writeStream
    .option("checkpointLocation", f"{chk}/<feed>/chk")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(f"{catalog}.helios_bronze.bronze_<feed>")
)
query.awaitTermination()
```

The rest of this document is what each of those lines is doing.

---

## What we set on the read

| Setting | What we set it to | What it does |
|---|---|---|
| `cloudFiles.format` | `json`, `csv` or `parquet` | the format of the files in that folder |
| `cloudFiles.schemaLocation` | `<chk>/<feed>/schema` | where the inferred schema is remembered between runs |
| `cloudFiles.schemaEvolutionMode` | `addNewColumns` | what to do when a new column turns up |
| `rescuedDataColumn` | `_rescued_data` | keeps values that do not fit the schema instead of dropping them |
| `header` | `true`, CSV only | the first line of the file holds the column names |

### cloudFiles.format

Set per feed. This picks the parser, so it has to match what is actually in the folder. Point the JSON parser at a Parquet file and every row comes back unparsed, because Parquet is a binary format and the JSON reader is looking for text.

Ours are JSON for `order_lines`, `orders`, `inventory`, `suppliers` and `warehouses`, Parquet for `customers`, `products` and `categories`, and CSV for `price_list`.

CSV needs one extra option. Without it the header line is treated as a row of data and the columns are named `_c0`, `_c1` and so on.

```python
.option("cloudFiles.format", "csv")
.option("header", "true")
```

### cloudFiles.schemaLocation

On the first run Auto Loader samples the files, works out the schema, and writes it to this path under a `_schemas` folder. Every later run reads that back instead of inferring again. Two things come from that. The columns stay the same from run to run even if a later file happens to be missing a field, and the run skips the sampling pass, which on a large feed is a scan of real data.

```python
.option("cloudFiles.schemaLocation", f"{chk}/order_lines/schema")
```

### cloudFiles.schemaEvolutionMode

What happens when a column we have never seen before appears in a file. We use `addNewColumns`, which is the default when you let Auto Loader infer the schema rather than handing it one. The next section walks what that does.

| Mode | What happens to a new column |
|---|---|
| `addNewColumns` | added to the stored schema, and the run stops so it can restart with the new schema |
| `addNewColumnsWithTypeWidening` | the same, and it also widens a type where it safely can, for example `int` to `long` |
| `rescue` | the schema never changes and the run never fails, the value goes into the rescued data column instead |
| `failOnNewColumns` | the run fails and stays failed until someone updates the schema or removes the file |
| `none` | the schema never changes and the column is ignored |

### rescuedDataColumn

Anything Auto Loader cannot fit into the schema goes here rather than being dropped. It catches three cases.

- A column present in the file that the schema does not have and evolution did not add.
- A value that cannot be parsed into the type the schema expects.
- A column whose name differs only by case.

The column holds a JSON string with the offending fields and the file they came from.

```json
{"quantity":"{\"units\":2}","_file_path":"/Volumes/labs_jane_doe/helios_raw/helios_landing/order_lines/batch_2/part-00000-....json"}
```

On a clean batch it is null on every row, and that is what the checks assert. The alternative to keeping this column is that an unparseable value is replaced by a null, which is indistinguishable from a field the source genuinely left empty. With the column set, the count below is zero on a good day and non zero on a bad one, and each of those rows still carries the original text and the file it came from.

```python
spark.table(f"{catalog}.helios_bronze.bronze_order_lines") \
     .filter("_rescued_data IS NOT NULL") \
     .count()
```

---

## When a new column turns up

Worked example. Say the supplier system starts sending `contact_email` on the `suppliers` feed.

1. The run reads the new file, sees a field it does not have, and writes `contact_email` into the schema at `cloudFiles.schemaLocation`.
2. That run then stops with an `UnknownFieldException` that names the field it did not expect. Nothing is lost and nothing is half written, because the write is one transaction.
3. The next run starts from the updated schema, reads the same files, and `contact_email` comes through as a normal column.
4. `mergeSchema` on the write side lets the Bronze table take the new column. Rows loaded before it existed read back with `null`.

So the cost of a schema change is one failed run. When this runs as a scheduled Job with retries, the retry does the restart and nobody is woken up.

Our schema is stable, so this is insurance rather than something you will watch happen. Compare it against the two alternatives. `failOnNewColumns` stops the feed dead until a person edits the schema, so a column nobody cares about can hold up the load overnight. `none` keeps the load running and drops the column on the floor, so the first anyone hears about it is a request for a field that was arriving all along and was never stored. `addNewColumns` costs one restart and loses nothing.

---

## Data types on the way in

For formats that do not carry data types, meaning JSON and CSV, **Auto Loader infers every column as a string**. A JSON file stores `"quantity": 2` as text with no declaration of what 2 is, and a CSV row is text by definition, so there is nothing in either file for Auto Loader to read a type from. It could guess from the values it samples, and by default it does not. Parquet is different, because a Parquet file carries the schema in its footer, types included.

So `bronze_order_lines` reads back like this, with `quantity` and `line_ts` as strings.

```
root
 |-- customer_id: string
 |-- line_ts: string
 |-- order_id: string
 |-- order_line_id: string
 |-- product_id: string
 |-- quantity: string
 |-- warehouse_id: string
 |-- _rescued_data: string
 |-- _ingest_ts: timestamp
 |-- _source_file: string
```

While `bronze_customers`, which comes from Parquet, keeps its dates as dates.

```
root
 |-- customer_id: string
 |-- customer_name: string
 |-- customer_type: string
 |-- tier: string
 |-- home_warehouse_id: string
 |-- signup_date: date
 |-- snapshot_date: date
 |-- _rescued_data: string
 |-- _ingest_ts: timestamp
 |-- _source_file: string
```

Two options can change that behaviour, and we deliberately use neither.

- `cloudFiles.inferColumnTypes` set to `true` makes Auto Loader sample the data and pick types for JSON and CSV as well.
- `cloudFiles.schemaHints` pins the type of one or more named columns, for example `"quantity INT, line_ts TIMESTAMP"`, and leaves the rest inferred.

We leave both off, for two reasons. A string column can hold anything the source sends, so nothing is lost at ingestion, whereas a value that fails to cast to an inferred type comes through as a null and the original text is gone. And typing at the front means the same rule is applied here and again in Silver, in two places that can drift apart. Casting once, in Silver, keeps every quality decision in the layer that owns them, where a row that fails is quarantined and counted rather than nulled.

One side effect to expect. JSON columns come back in alphabetical order, because a JSON file has no column order to preserve and Spark sorts the field names it discovers. Parquet and CSV both record an order, so they keep the one they were written in.

---

## What we set on the write

| Setting | What we set it to | What it does |
|---|---|---|
| `checkpointLocation` | `<chk>/<feed>/chk` | the record of which files have been processed |
| `mergeSchema` | `true` | lets the target table take a new column |
| `trigger` | `availableNow=True` | process what has landed, then stop |
| `toTable` | `bronze_<feed>` | writes to a managed Delta table in Unity Catalog |

### checkpointLocation

The list of files already processed, written as part of the same commit that writes the rows. That is what makes a re-run safe. The stream cannot end up having recorded a file as done without its rows being in the table, or the other way round, so a run that dies halfway leaves nothing to unpick by hand. Like the schema location it is per feed, because it is one list of files and two feeds sharing it would each skip the other's.

### mergeSchema

`cloudFiles.schemaEvolutionMode` governs the reader. This governs the table. They are separate because a Delta table has a schema of its own, enforced on write, so a column the reader now accepts is still rejected by the table until the table's schema is updated too. With `mergeSchema` set, the write adds the column to the table instead of failing.

### trigger(availableNow=True)

The trigger decides whether this is a batch job or a running service.

| Trigger | Behaviour |
|---|---|
| `availableNow=True` | reads everything currently available in as many micro batches as it needs, then stops |
| `processingTime="30 seconds"` | runs forever, waking up on that interval |
| no trigger | runs forever, in continuous micro batches |

We use `availableNow`, so this is a scheduled batch job that happens to be written with the streaming API. It starts, drains the new files, and shuts down. We get the incremental bookkeeping of a stream without paying for a cluster that never sleeps.

### toTable and awaitTermination

`toTable` writes to a managed Delta table, which puts the storage, the permissions and the lineage under Unity Catalog rather than leaving files in a path somebody has to remember to govern separately.

`awaitTermination()` blocks until the load has finished. Without it the cell returns immediately, the stream is still running in the background, and the next step reads a table that is half loaded.

```python
query = stream.writeStream ... .toTable(f"{catalog}.helios_bronze.bronze_order_lines")
query.awaitTermination()
```

---

## The three columns we add

On top of whatever the source sent, every Bronze row carries three columns of ingest metadata.

| Column | Type | Where it comes from |
|---|---|---|
| `_ingest_ts` | timestamp | `current_timestamp()` at load |
| `_source_file` | string | `_metadata.file_path` |
| `_rescued_data` | string | the `rescuedDataColumn` option |

`_metadata` is a hidden column Spark exposes on any file based read, carrying the file path, name, size and modification time. Spark already knows all of that from listing the files, so selecting from it reads no extra data. We keep the path.

```python
.withColumn("_ingest_ts", current_timestamp())
.withColumn("_source_file", col("_metadata.file_path"))
```

A row in `bronze_order_lines` after a batch one load.

| column | value |
|---|---|
| order_line_id | OL-1-000001 |
| order_id | ORD-000001 |
| customer_id | CUST-0036 |
| product_id | PRD-017 |
| warehouse_id | DEP-01 |
| quantity | 2 |
| line_ts | 2257-03-01T13:47:04.000Z |
| _ingest_ts | 2026-07-26 09:14:22.481 |
| _source_file | /Volumes/labs_jane_doe/helios_raw/helios_landing/order_lines/batch_1/part-00000-....json |
| _rescued_data | null |

`_source_file` is what makes a Bronze row auditable. When a figure in a report looks wrong, you filter Bronze down to the rows behind it, read the file paths out of that column, and open those files. Without it, tracing one row back means searching every landing folder for the feed by hand, and there is a new one of those every day.

---

## What a run looks like

The whole point of the checkpoint, in numbers.

| Run | What is in the landing folder | What Auto Loader reads | Rows in `bronze_order_lines` |
|---|---|---|---|
| First run after batch one lands | `batch_1/` | `batch_1/` | 50,626 |
| Run it again, nothing new landed | `batch_1/` | nothing | 50,626 |
| Batch two lands, run again | `batch_1/`, `batch_2/` | `batch_2/` only | 113,105 |

Look at the second run. It reads nothing and writes nothing, because the checkpoint already lists `batch_1/` as processed, so there is no file left for the run to pick up. Two things follow from that. The load can be re-run at any time without duplicating rows, which is what makes it safe to put on a schedule with retries. And the work a run does is set by how much landed since the last one, not by how much is in the folder, so run time stays flat whether that feed holds two batch folders or two hundred.

---

## Every setting in one place

| Setting | Side | Our value | What it does |
|---|---|---|---|
| `cloudFiles.format` | read | `json` / `csv` / `parquet` | the format of the files in the folder |
| `cloudFiles.schemaLocation` | read | `<chk>/<feed>/schema` | remembers the inferred schema between runs |
| `cloudFiles.schemaEvolutionMode` | read | `addNewColumns` | adds an unexpected column, stops the run once |
| `cloudFiles.inferColumnTypes` | read | not set | would type JSON and CSV columns instead of leaving them as strings |
| `cloudFiles.schemaHints` | read | not set | would pin the type of named columns |
| `rescuedDataColumn` | read | `_rescued_data` | keeps values that do not fit the schema |
| `header` | read | `true` on CSV | first line holds the column names |
| `checkpointLocation` | write | `<chk>/<feed>/chk` | which files have already been processed |
| `mergeSchema` | write | `true` | lets the Bronze table take a new column |
| `trigger` | write | `availableNow=True` | drains what has landed, then stops |
| `toTable` | write | `bronze_<feed>` | managed Delta table in Unity Catalog |

---

## Where to read more

- [What is Auto Loader?](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)
- [Configure schema inference and evolution in Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema)
- [Auto Loader options](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/options)
- [File metadata column](https://docs.databricks.com/aws/en/ingestion/file-metadata-column)
- [Configure Structured Streaming trigger intervals](https://docs.databricks.com/aws/en/structured-streaming/triggers)
