# Helios Trading Corporation: Operational Serving

The concepts behind Section 6. Gold is built, the semantic layer defines every metric once, and Genie and the dashboards answer from it. All of that is analytics: a person asks a question, waits a moment, reads the answer. Section 6 serves the same numbers to software. A depot manager opens an operations console, picks a depot, and the page renders its figures at once, click after click, all day. A query that takes seconds makes that page feel broken, so this section syncs a small curated slice of Gold into Lakebase, the managed Postgres database on Databricks, and a live app reads the copy in milliseconds. 

---

## Analytical and operational reads

The Gold star schema and the metric views are built for analytical reads. Delta stores data in columnar files, keeping every value of a column together, so an aggregate reads only the columns it needs and scans millions of rows quickly, and the SQL warehouse is an engine designed for exactly that scan. The cost is a fixed overhead per query, planning and IO that add up to a second or two even when the result is a single row. For a dashboard refresh or a Genie question that overhead is invisible, because the person asked for a computation across the whole history and a short wait is expected.

An operational read is a different job. The console needs one row, found by key, returned fast enough that the page never appears to load, and there may be many users clicking at once. A row store like Postgres keeps each row's values together and finds a key through an index, so a keyed read touches a handful of pages and returns in single digit milliseconds, with no scan at all. Running a fresh warehouse aggregate on every click would spend seconds and serverless compute to produce a number that has not changed since the last click.

|  | Analytical | Operational |
|---|---|---|
| Typical query | aggregate millions of rows | fetch one row by key |
| Acceptable latency | seconds | milliseconds |
| Storage layout | columnar (Delta) | row oriented (Postgres) |
| Reader | analysts, dashboards, Genie | an application, on every click |

Neither store replaces the other. The question a design answers is which reads go where, and Section 6 is the pattern for routing the operational ones.

---

## Lakebase

Lakebase is a managed Postgres database inside the Databricks platform. It is real Postgres, so `psql`, any Postgres driver and any ORM connect to it the way they would to any other Postgres server. What the platform adds is the integration: it is governed through Unity Catalog, billed as serverless compute, and kept in step with Delta tables by a managed sync, so serving lakehouse data operationally does not need an export job you write and schedule yourself.

Everything in Lakebase belongs to a project. A project holds branches, starting with one named `production`. A branch holds databases, with `databricks_postgres` created by default, and a compute endpoint that serves its connections. An app is given the endpoint's host and connects with ordinary Postgres credentials.

---

## Synced tables

A synced table is a Postgres copy of a Unity Catalog table, kept current by a managed pipeline. You choose the source table, a primary key, and a sync mode, and the platform creates both the Postgres table and the Lakeflow pipeline that maintains it. The primary key is how the pipeline matches rows, so an update to a source row updates the same row in the copy instead of inserting a duplicate.

| Mode | How it copies |
|---|---|
| Snapshot | the full table, each time it runs |
| Triggered | only the rows changed since the last run, on demand or on a schedule |
| Continuous | a stream that applies changes within seconds, on compute that never stops |

Triggered and continuous mode read the source table's Change Data Feed, so the source must be created with `delta.enableChangeDataFeed` on. That feed is what lets a refresh move only what changed rather than recopying the table.

What to sync is the design decision that comes with the tool. Postgres finds a row fast; it is not the engine to aggregate fifty million order lines, and syncing raw facts would move the analytical workload into the copy rather than the result. So the labs sync one small serving table, one row per depot, aggregated to exactly the grain the console reads, and built from the Section 5 metric views so the console, Genie and the dashboards all report the same figures. The lakehouse stays the system of record, and if a served number is wrong the fix goes into Gold and syncs through, never into the copy by hand.

The connection also works in reverse. Registering the Lakebase database as a Unity Catalog catalog makes its tables queryable from the lakehouse, so a notebook can read the serving copy with ordinary SQL and confirm it matches the source.

---

## Branches

A branch is a copy on write clone of a database. Creating one is instant and stores nothing new, because the branch shares its parent's storage until something is written, and only the changed pages are then stored separately. That gives you a full copy of production data to test a schema change or a heavy query against, at almost no cost, with the live serving copy untouched. A branch can be reset from its parent to discard an experiment, or deleted when it has served its purpose. It is the same instinct that kept a development pipeline alongside production in Section 4, applied to the operational store.

---

## Autoscaling and scale to zero

The compute endpoint autoscales within a range you set, growing as CPU and memory demand rises and shrinking as it falls, so a spike in console traffic is absorbed without anyone resizing a server. When there are no connections at all, scale to zero suspends the compute entirely, and the next query wakes it in a few hundred milliseconds.

The consequence for cost is that an idle project spends nothing on compute, which is why a lab can leave one running. The consequence for latency is that the first read after an idle period pays the wake up time, a detail to account for when an operational SLA promises a response time on every request, not just the warm ones.

---

## When a dedicated operational layer is the right answer

Add one when software is the reader: an app, an API, an agent, anything that issues many small keyed reads and has a latency budget per read. The shape of the solution stays constant. The pipelines keep computing the numbers in the lakehouse, a sync serves a curated copy at the grain the reader needs, and the reader never touches the analytical store.

When a person is the reader, the warehouse already serves them: a dashboard tolerates seconds, and adding an operational copy would add a second store to govern and keep fresh for no gain. The copy is only worth its upkeep once per read latency is the requirement.

---

## References

- [Lakebase Postgres](https://docs.databricks.com/aws/en/oltp/)
- [Serve lakehouse data with synced tables](https://docs.databricks.com/aws/en/oltp/projects/reverse-etl)
- [Register a Lakebase database in Unity Catalog](https://docs.databricks.com/aws/en/oltp/projects/register-uc)
- [Branches](https://docs.databricks.com/aws/en/oltp/projects/branches)
- [Autoscaling](https://docs.databricks.com/aws/en/oltp/projects/autoscaling)
- [Scale to zero](https://docs.databricks.com/aws/en/oltp/projects/scale-to-zero)
