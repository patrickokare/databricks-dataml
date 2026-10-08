# Formula 1 Racing Lakehouse

> 🚧 **In progress:** design complete. The full specification, drop catalogue and answer key will be published here as each layer is built.

Kimball star schema over Ergast F1 data (1950–2020), kept correct through 13 dated 2021 deliveries: restatements, SCD2 constructor renames, late-arriving drivers, partial deliveries and a byte-for-byte re-send.

See the [portfolio overview](../README.md#kimball-patterns-coverage) for how this project fits with the others.

## Planned contents

```text
schema/    Unity Catalog DDL: catalog, schemas, Volumes, tables, grants
bundles/   Databricks Asset Bundle: databricks.yml, jobs/pipelines, source code
docs/      spec, ADRs, diagrams, answer key, performance log
```
