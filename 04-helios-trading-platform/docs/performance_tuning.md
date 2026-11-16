# Performance tuning on Databricks and Delta Lake

Concepts, how they apply to the Helios Trading Platform, and interview-ready summaries.

> **First rule: measure before you optimise.** Find the slow or expensive step with the query profile, the Spark UI and `system.billing.usage`, change one thing, and measure again. "Measured, not assumed."

---

## 1. Adaptive Query Execution (AQE)

AQE re-plans a query **while it runs**, using real statistics from completed shuffle stages instead of up-front estimates. It is on by default in Databricks, including serverless.

- **Coalesces shuffle partitions**: merges many tiny post-shuffle partitions, so `spark.sql.shuffle.partitions` rarely needs hand-tuning.
- **Switches join strategy**: converts a sort-merge join to a broadcast join when one side turns out to be small at runtime.
- **Handles skewed joins**: splits oversized partitions (for example one very large customer) so a single task does not stall the stage.

*Interview line:* "AQE fixes plans based on what the data actually looks like at runtime. It is on by default, so my job is mostly not to defeat it, for example with forced hints or `.collect()` round trips."

## 2. Data skipping: why file layout matters

Delta stores min/max statistics per data file (first 32 columns by default). A filter such as `date_key = 22570301` skips every file whose range cannot contain the value. Partitioning, Z-ordering and liquid clustering all exist to make those ranges **narrow**, so more files are skipped.

## 3. Partitioning (older approach)

Splits a table into folders by a column value.

- Only worth it for very large tables (roughly a terabyte or more) on a low-cardinality column used in almost every filter.
- Over-partitioning creates thousands of small files, which is slower, not faster.
- Changing the partition column means rewriting the whole table.

## 4. Z-ordering (legacy approach)

`OPTIMIZE ... ZORDER BY (cols)` co-locates rows with similar values in a few columns, narrowing min/max ranges.

- **Not incremental**: each run rewrites the data; new writes are not Z-ordered until the next run.
- Each extra column makes it less effective.
- Cannot be used on partition columns.

## 5. Liquid clustering (current recommendation)

Declare `CLUSTER BY (cols)` on the table and Delta keeps data clustered by those keys.

- **Incremental**: `OPTIMIZE` only clusters data that is not clustered yet.
- **Keys can change** with `ALTER TABLE ... CLUSTER BY` without rewriting existing data.
- Up to four keys; handles **high-cardinality** columns, where partitioning fails.
- **`CLUSTER BY AUTO`** (with predictive optimization) chooses keys from real query patterns.
- Replaces partitioning and Z-order on new tables; they cannot be combined on the same table.
- Works on pipeline streaming tables and materialized views.

*Interview line:* "For new tables I use liquid clustering on the columns I filter and join on most, instead of partitioning or Z-order. It is incremental, handles high-cardinality keys, and I can change keys later without a rewrite."

## 6. OPTIMIZE and file sizing

Streaming and frequent MERGE create many small files, and every file adds open and metadata overhead.

- `OPTIMIZE` compacts small files, and performs clustering on liquid-clustered tables.
- Optimized writes and auto compaction handle much of this while writing.
- **Predictive optimization** on Unity Catalog managed tables (where enabled) runs `OPTIMIZE`, `VACUUM` and statistics collection automatically.
- Lakeflow pipelines run this maintenance on the tables they manage.

## 7. VACUUM

Deletes data files no longer referenced by the table (old versions left by MERGE and overwrite) once they are older than the retention period (7 days by default).

- **Saves storage cost**; it does not directly speed up queries (that is `OPTIMIZE`).
- **Limits time travel**: you cannot time-travel or `RESTORE` to versions whose files were vacuumed. This matters for Bronze -> Silver replay strategies.
- Do not lower retention below 7 days without care: streaming readers or long-running queries may still need old files.

## 8. Faster writes: MERGE and deletion vectors

- **Deletion vectors**: MERGE, UPDATE and DELETE mark rows as removed instead of rewriting whole files. On by default for new tables in recent runtimes, which makes `upsert` cheaper.
- **Prune the target**: include a clustering column in the merge condition (for example a recent `date_key` range) so Delta skips most target files.

## 9. Joins

- **Broadcast small tables** (under about 10 MB by default) so the large side does not shuffle; AQE does this at runtime too.
- **Range joins**: for "value between start and end" joins with no equality key, Databricks has a range join optimization. The Helios price join also matches on `product_id`, so it is mainly an equality join plus a filter over a few versions per product.
- **Fan-out** (duplicate join matches) is a performance bug as well as a correctness bug: it multiplies rows and shuffle volume.

## 10. Engine and code habits

- **Photon** (vectorised engine) is on with serverless. Native Spark functions run in Photon; Python UDFs do not, so prefer built-ins.
- Avoid `.collect()`, repeated `.count()` and `.toPandas()` on large data, and `.cache()` unless measured.
- Keep statistics current with `ANALYZE TABLE` (or predictive optimization) so the optimizer estimates well.

---

## Applying this to Helios

At the current size (about 180,000 order lines) layout changes will not be measurable. The biggest wins are in **how much work each run does**, not file layout. The layout points matter at production scale and in interviews.

| Area | What happens now | Improvement |
|---|---|---|
| `silver_orders` (Lab 3.8 Task 3) | Re-reads all of `bronze_orders` every run | Stream the CDC feed with a sequence-aware merge, or AUTO CDC (Module 04), so only new changes are processed |
| `fact_order_lines` (Lab 3.10) | Joins and MERGEs every Silver line every run | Process only new or changed Silver rows, for example via Change Data Feed on Silver |
| Full-snapshot dimensions | Read every snapshot ever landed in Bronze | Filter to the latest snapshot first: faster, and it also makes source deletions disappear correctly |
| `fact_order_lines` layout | No clustering | Liquid clustering on common report filters, e.g. `date_key`, `product_id`, or `CLUSTER BY AUTO` |
| `fact_order_lines` MERGE | Matches on `order_line_id` only | Add a recent `date_key` range to the merge condition (with clustering on `date_key`) so MERGE prunes old files |
| `fact_inventory` | No clustering | Cluster on `warehouse_id`, `product_id`: both the MERGE key and the usual stock filters |
| Dimensions, price SCD, `fact_returns` | Small tables | Leave unclustered; they are broadcast in joins automatically |
| Bronze tables | Append-only, small files from micro-batches | Auto compaction or predictive optimization, plus `VACUUM` with retention that still covers the replay window |
| Section 4 pipelines | Engine manages checkpoints and maintenance | Add `CLUSTER BY` to pipeline table definitions; leave `OPTIMIZE`/`VACUUM` to the pipeline |

### Key Note on perfromance Improvement

"My first Gold build recomputed everything each run. Before tuning file layout, I made the pipeline incremental: CDC feeds through sequence-aware merges, Gold fed from Silver's change feed. Then I added liquid clustering on the fact's main filters and a date-range predicate in the MERGE so it only touches recent files. I measured each step with the query profile and `system.billing`."

---

## Q&A

| Question | Short answer |
|---|---|
| OPTIMIZE vs VACUUM? | OPTIMIZE compacts and clusters files for speed. VACUUM deletes unreferenced old files for storage cost and limits time travel. |
| Z-order vs liquid clustering? | Z-order is a full rewrite each run with fixed columns. Liquid is incremental, keys can change, and it handles high cardinality. Liquid is preferred for new tables. |
| When would you partition? | Rarely: very large tables, low-cardinality column used in almost every filter. Otherwise liquid clustering. |
| What does AQE do? | Re-plans at runtime: coalesces shuffle partitions, switches to broadcast joins, splits skewed partitions. |
| Why are small files bad? | Each file adds open and metadata overhead, and min/max stats get less selective. Fix with compaction. |
| How do you make MERGE faster? | Deletion vectors, a clustered target, and a clustering-key predicate in the merge condition so fewer target files are scanned. |
