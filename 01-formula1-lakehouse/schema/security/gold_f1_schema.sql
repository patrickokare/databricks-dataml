CREATE SCHEMA IF NOT EXISTS gold_f1
MANAGED LOCATION 'abfss://<container>@<storage_account>.dfs.core.windows.net/gold_f1'
COMMENT 'Gold layer schema for Formula 1 business-level aggregates and analytics'