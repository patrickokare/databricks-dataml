CREATE SCHEMA IF NOT EXISTS bronze_f1
MANAGED LOCATION 'abfss://<container>@<storage_account>.dfs.core.windows.net/bronze_f1'
COMMENT 'Bronze layer schema for Formula 1 raw data ingestion'