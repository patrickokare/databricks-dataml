CREATE SCHEMA IF NOT EXISTS silver_f1
MANAGED LOCATION 'abfss://<container>@<storage_account>.dfs.core.windows.net/silver_f1'
COMMENT 'Silver layer schema for Formula 1 cleaned and transformed data';
