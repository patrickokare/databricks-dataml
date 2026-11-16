# Helios Trading Corporation: Lakeflow Spark Declarative Pipeline Concepts

The concepts behind Section 4. We rebuild the same medallion as Section 3, same contract, same target tables, on Lakeflow Spark Declarative Pipelines, and everything in the coming labs is one of the ideas below. The sections follow the lecture in the same order, so whatever is being talked through on screen has a section here with the detail in it.

---

## From imperative to declarative

In Section 3 every table came with its own machinery: a stream with a checkpoint, a MERGE with its keys, a Job task slotted into the right position in the graph. We wrote what each table is and also how and when to build it.

Declarative keeps the first half and hands over the second. You declare each table as a function that returns a DataFrame, and the engine reads all the declarations, works out which table depends on which, and derives the run order, the checkpoints, the retries and the restarts itself.

One naming note. Lakeflow Spark Declarative Pipelines is the current name for what Databricks used to call Delta Live Tables. The Python interface is the `pyspark.pipelines` module; the old `dlt` module still runs but is deprecated, so this is the import every source notebook starts with.

```python
from pyspark import pipelines as dp
```

---

## The pipeline

The pipeline is the unit of development and execution, and the container for everything we declare. It points at one or more source notebooks; the engine parses them all together and wires the dependency graph by table name. A source notebook is never run top to bottom the way a normal notebook is, which is why it looks different from every notebook in Section 3.

Settings that lived in our code now live on the pipeline:

| Setting | In this course |
|---|---|
| Target catalog and schema | your `labs_<user>` catalog |
| Compute | serverless, the only option on Free Edition |
| Mode | triggered: run, process what is new, stop. Continuous is for seconds-level latency and costs accordingly |
| Configuration | key value pairs the source code reads back with `spark.conf.get` |

The configuration map matters here because of an identity wrinkle: inside a pipeline, `current_user()` returns the engine's system identity, not you. So the lab driver passes your catalog in as a configuration value, and the source notebooks read it back rather than deriving it.

Free Edition allows one active pipeline, so every Section 4 lab finds and rebuilds the same one rather than creating its own.

---

## Flows

Underneath everything the engine runs is a flow: read a source, apply the logic, write a target. Most of the time you never see one, because a decorated function declares a dataset and the flow that feeds it in one gesture.

Flows become visible in two places, both coming up. A table can be fed by more than one flow, which is how the quarantine pattern keeps dropped rows. And change data capture is a specialist flow you attach to a table explicitly. A flow can also write to a target outside the pipeline, called a sink, such as a Kafka topic; we do not use sinks in this course.

---

## Streaming tables

A streaming table is for data that arrives as new rows. It processes an append-only source with each record handled exactly once, so a run picks up what landed since the last run and touches nothing it has already processed. That makes it the shape for ingestion and for row-level transforms such as typing and dedup.

Bronze is the clearest case. Auto Loader lives inside a streaming table declaration, and everything we managed by hand in the Jobs build is owned by the engine:

```python
@dp.table(name="<bronze table>")
def ingest():
    return (
        spark.readStream.format("cloudFiles")
        ...
        .load("<landing folder>")
    )
```

| In Section 3 we managed | Now owned by |
|---|---|
| the checkpoint location | the engine |
| the schema location | the engine |
| starting and awaiting the stream | the engine |
| run order across tables | the graph |
| retries after a failure | the pipeline |

The reader and its options are unchanged. What disappears is the machinery around it.

---

## Materialized views

A materialized view is the stored result of a query, recomputed as needed so it reflects the current state of its inputs. You declare the question once, and keeping the answer current becomes the engine's problem, including working out when an incremental refresh is enough and when to recompute.

That makes it the shape for the other half of the medallion: joins, aggregations, and conforming a full snapshot. Our reference tables, dimensions and facts are all materialized views.

The rule that decides between the two dataset types:

| The table is | Declare it as |
|---|---|
| rows arriving, each to be processed once | a streaming table |
| a question over other tables: a join, an aggregate, a conform | a materialized view |

---

## Views

A view is evaluated on demand and never persisted, and only datasets inside the pipeline can read it. It is the shape for an intermediate step with no business reader: in our build, the change feeds are typed in a view before the CDC flow picks them up, so the typing is written once without a table existing for it.

---

## Dataset function rules

A dataset function must only return a DataFrame. No `collect()`, `count()` or `toPandas()`, no `save()` or `saveAsTable()`, no `writeStream`, checkpoint or `start()`.

The reason is what the function is for. The engine parses it to plan the graph, then executes it on its own schedule; the function is a definition, not a script. An action would pull data during planning, before the graph the query depends on exists. A write would name a target the engine does not know about and fight it for control of the table it owns.

The consequence for how the labs are shaped: a source notebook contains imports, the configuration read, and dataset definitions, nothing else. Every query, count and check happens in the driver notebook, after the run.

---

## Expectations

An expectation is a quality rule declared on the dataset it protects: a name plus a boolean expression, checked against every row on the way in. The contract's quality rules stop being filter code we maintain and become declarations the engine enforces.

There are three severities, and the choice between them is a real engineering decision:

| Severity | Decorator | The failing row | The run |
|---|---|---|---|
| warn | `@dp.expect` | written, and counted | continues |
| drop | `@dp.expect_or_drop` | removed, and counted | continues |
| fail | `@dp.expect_or_fail` | not written | halts |

Warn is for a rule you want to watch without acting on the data. Drop is for rows the contract says must not reach the clean table. Fail is for a condition that should never be true, where continuing would publish wrong numbers; it stops the pipeline, so it is also the severity that can turn a data surprise into an outage. The engine records the pass and fail counts for every rule on every run.

Drop does not mean delete. The quarantine rule from Section 3 carries straight over as two flows off the same source: the clean table drops the failing rows, and a second table selects only those rows. One pass, and nothing is lost.

---

## AUTO CDC

The largest single collapse of Section 3 code. The foreachBatch MERGE that worked out each order's latest state from the change feed, and the window plus MERGE that built each SCD2, each become one directive.

The pattern has two steps: declare the target, then attach the flow. This is the place where a flow is written out explicitly.

```python
dp.create_streaming_table(name="<target>")

dp.create_auto_cdc_flow(
    target="<target>",
    source="<typed change view>",
    keys=["<business key>"],
    sequence_by=col("<ordering column>"),
    stored_as_scd_type=1,
)
```

What each argument decides:

- `keys` is the business key the changes describe, one output row (or one version history) per key.
- `sequence_by` is which change wins, and it is what makes out of order arrival safe: the engine applies the highest sequence value per key no matter what order the rows turned up in. Our orders sequence by `change_seq`, not `change_ts`, for exactly the reason the Section 3 build did: the timestamps can arrive out of order, the sequence number cannot.
- `stored_as_scd_type` is what the target keeps. Type 1 keeps current state only, which is our `silver_orders`. Type 2 keeps every version with its validity window, which is our price history.
- `apply_as_deletes` names the rows that are deletions, as an expression such as `col("op") == "DELETE"`, so a purge at the source becomes a removal in the target.
- `except_column_list` names columns that stay behind, such as the CDC `op` flag, which is bookkeeping for the feed rather than a fact about the order.

For type 2 the engine adds two columns, `__START_AT` and `__END_AT`, typed like the `sequence_by` column, and the current version is the one whose `__END_AT` is null. It does not produce the contract's `effective_from`, `effective_to` or `is_current`; those are derived from the engine's columns in a materialized view downstream, which is why our SCD2s come in pairs.

---

## CDC from snapshots

The customers feed has no change rows and no version dates; it is a full snapshot every day. There is nothing to `sequence_by`, so it uses the second form, `create_auto_cdc_from_snapshot_flow`: you hand the engine the snapshots in order, it diffs each against the last, and it opens and closes versions wherever a customer's attributes changed.

The two forms exist because sources really do split this way:

| Feed | What it carries | Which form |
|---|---|---|
| price_list | explicit `effective_from` version dates | `create_auto_cdc_flow`, sequenced by the date |
| customers | a full daily snapshot, no version dates | `create_auto_cdc_from_snapshot_flow` |

---

## References

- [Lakeflow Spark Declarative Pipelines concepts](https://docs.databricks.com/aws/en/ldp/concepts/)
- [Python language reference](https://docs.databricks.com/aws/en/ldp/developer/python-ref)
- [Streaming tables](https://docs.databricks.com/aws/en/dlt/streaming-tables)
- [Manage data quality with pipeline expectations](https://docs.databricks.com/aws/en/dlt/expectations)
- [The AUTO CDC APIs](https://docs.databricks.com/aws/en/ldp/cdc)
