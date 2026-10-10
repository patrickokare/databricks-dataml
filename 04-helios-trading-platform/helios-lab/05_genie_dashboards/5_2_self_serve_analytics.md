# Helios Trading Corporation: Self Serve Analytics

The concepts behind Section 5. Gold is finished and correct, but no business user can read a star schema, and every team that queries it invents its own version of revenue. So this section builds a second set of views. They read the Gold tables, and each one holds a business metric written out once. Genie reads those views to answer questions asked in plain English, and the AI/BI dashboards read the same ones for the charts. The sections below follow the lecture in the same order.

---

## Views

A Unity Catalog view is a stored query, not stored data. Creating one registers the query text in the metastore and writes nothing, so every read runs the query against the tables underneath as they are now. It is read only, and it can be granted like a table.

That makes a view the place to give a business user something readable. Our facts carry keys, not names, so a row in `fact_order_lines` has a warehouse id on it and no depot name, and getting that name means joining to `dim_warehouse`. A business user is not going to write that join. Written once in a view, what they query is one flat table with the depot name, the product name and the customer tier already on the row.

---

## Metric views

A view still returns rows, so the aggregate is written by whoever queries it. A business user asking for revenue has to know it is `SUM(gross_amount)`, and every dashboard, notebook and query ends up carrying its own version of that.

A metric view stores the aggregate instead of the rows. You declare **dimensions**, the columns a caller may group by, and **measures**, named aggregates such as `SUM(gross_amount)`, each written once. Nothing is computed when you create it. The caller asks for a measure and the engine computes it at the grain the query asked for, so `Revenue` is one definition whether you want it for the whole company, for one depot, or for a depot and a day.

You read a measure with the `MEASURE()` function.

```sql
SELECT `Depot`, MEASURE(`Revenue`)
FROM <catalog>.<schema>.sales_mv
GROUP BY ALL
```

`SELECT *` does not work here. A measure only evaluates inside `MEASURE()`, so you list the dimensions you want and wrap each measure. `GROUP BY ALL` then groups by whatever dimensions you selected, which saves naming them twice.

---

## Defining a metric view

The definition is YAML. It names the source, then the dimensions and measures.

```yaml
version: 1.1
source: "SELECT * FROM <catalog>.<schema>.<name>_vw"
comment: "<what the view covers, and its grain>"
filter: <a boolean expression every query is held to>
dimensions:
  - name: <Dimension Name>
    expr: <a column or an expression>
    comment: "<what it is>"
measures:
  - name: <Measure Name>
    expr: <an aggregate over the source columns>
```

| Key | Required | What it sets |
|---|---|---|
| `version` | yes | the version of the YAML specification, currently `1.1` |
| `source` | yes | the data underneath: a table, a view, another metric view, or a query over one |
| `dimensions` | one of the two | the columns a caller may group by, each a `name` and an `expr` |
| `measures` | one of the two | the named aggregates, each a `name` and an `expr` |
| `comment` | no | the description Unity Catalog stores and Genie reads |
| `filter` | no | a boolean expression applied to every query against the view, so rows it excludes cannot be reached through this view at all |
| `joins` | no | star and snowflake joins, for when you would rather join here than in the source |

Four details decide whether the YAML parses and whether the measures stay honest.

- Quote a query used as `source`. An unquoted `*` opens a YAML alias, so `SELECT * FROM ...` is read as something else entirely. The same applies to any expression containing a colon.
- `dimensions` and `fields` are the same key. The documentation now prefers `fields`, the low-code editor in Catalog Explorer labels them Fields and still writes `dimensions`, and both are accepted.
- `FILTER (WHERE ...)` after an aggregate counts only the rows that match, which is how a rate is written: `COUNT(1) FILTER (WHERE status = 'CANCELLED') / COUNT(1)`.
- A measure can be built from measures defined above it, by calling `MEASURE()` inside its own `expr`, such as `MEASURE(Revenue) / MEASURE(Orders)`. Compose rather than re-derive, because a correction to `Revenue` then reaches every measure standing on it.

A metric view cannot be joined to a table directly. Wrap its query in a CTE and join that.

---

## Three ways to create one

### In Catalog Explorer

1. Open **Catalog** and find the table the metric view will read from.
2. Choose **Create > Metric view**, name it, pick the catalog and schema, and click **Create**.
3. The editor opens on the UI tab, with every source column already added as a field and a sample `COUNT(*)` measure.
4. Build the dimensions and measures from the drop-downs. **Preview** on an expression runs it and shows you the result.
5. **Save** commits it.

### In YAML

The `<>` button in that editor switches between the drop-downs and the YAML, which is the same definition written as text. Nothing is lost either way, so you can start in the drop-downs and finish in the YAML, or paste a whole definition straight in.

### In SQL

The YAML goes into a `CREATE VIEW` with the `WITH METRICS` clause, between `$$` delimiters, in place of the usual query. The labs build the layer from a notebook, so each metric view is that statement passed to `spark.sql`.

```python
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.helios_semantic.<name>_mv
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
source: "SELECT * FROM {catalog}.helios_semantic.<name>_vw"
...
$$
""")
```

Writing it as code is what makes the layer reproducible, and it is why the labs take this path rather than the editor. The whole semantic layer rebuilds from one notebook, in order, against whichever catalog you point it at.

To change a definition afterwards, `ALTER VIEW <name> AS $$ ... $$` edits one in place. It replaces the entire definition, and it drops any Unity Catalog comment you have not restated in a `comment:` field. From a notebook that already holds the full YAML, `CREATE OR REPLACE VIEW` avoids the question.

Either path needs `SELECT` on the source, `CREATE TABLE` and `USE SCHEMA` on the target schema, and `USE CATALOG` on its catalog. `CREATE TABLE`, not a view privilege, is the one that catches people out.

---

## Why each metric view reads a plain view

A metric view reads a single source, but our measures need columns from several dimensions at once, a depot name, a category, a customer tier. So each subject area is built as a pair: a plain view (`_vw`) that joins the fact to its dimensions into one flat row, and a metric view (`_mv`) whose source is that view. The joins then live in one place, where a fan-out is visible and fixable, and the `_mv` is a flat list of names and expressions with no join logic in it at all.

---

## Genie spaces

A Genie space is a chat interface over a chosen set of data. A business user asks a question in plain English and gets back an answer, a chart, and the SQL that produced them. It answers from tables and views registered in Unity Catalog, and metric views are first class data for it: the dimension names, the measure names and the comments become the vocabulary it reasons with, so a question about margin resolves to the governed `Gross Margin` rather than to a `SUM` it invented.

A space takes up to 30 tables or views, and the guidance is to aim for five or fewer.

One naming note. Databricks has renamed these in the documentation, where a Genie space is now called a Genie Agent. This course uses Genie space throughout.

---

## The dashboards read the same metric views

An AI/BI dashboard adds a metric view as a dataset directly. It arrives carrying its dimensions and measures, and the dashboard applies `MEASURE()` itself, so a widget is built by picking a dimension and a measure instead of writing SQL.

This is what makes the two surfaces agree. A chart and a Genie answer that both read `Revenue` cannot show a stakeholder two different numbers, and a definition that turns out to be wrong is corrected in one place.

---

## Best practices

| Practice | What it changes |
|---|---|
| Point a space at a few curated metric views, not a schema of raw tables | fewer, wider, business named assets leave fewer wrong ways to answer a question |
| Comment every table, column, dimension and measure | the comments are read as documentation, so they are the cheapest accuracy fix available |
| Define business terms as SQL, not prose | a measure or a SQL expression is a definition that gets executed; a text instruction is a hint that may not be followed |
| Keep instructions and example queries consistent | an instruction that contradicts an example, such as rounding to two places where the example does not, makes the answer unpredictable |
| Read the generated SQL | it is shown with every answer, and it is the only way to tell a correct answer from a plausible one |

---

## References

- [What is a view?](https://docs.databricks.com/aws/en/views/)
- [Metric views](https://docs.databricks.com/aws/en/uc-semantics/metric-views)
- [Metric view YAML reference](https://docs.databricks.com/aws/en/uc-semantics/metric-views/yaml-reference)
- [Create a metric view](https://docs.databricks.com/aws/en/uc-semantics/metric-views/create)
- [Query metric views](https://docs.databricks.com/aws/en/uc-semantics/metric-views/query)
- [CREATE VIEW, and the WITH METRICS clause](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-view)
- [Genie](https://docs.databricks.com/aws/en/genie/)
- [Create and manage a Genie space](https://docs.databricks.com/aws/en/genie-agents/set-up)
- [Curate an effective Genie space](https://docs.databricks.com/aws/en/genie-agents/best-practices)
