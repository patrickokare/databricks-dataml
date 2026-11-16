# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 6.6: Deploy the Depot Operations Console
# MAGIC
# MAGIC In Lab 6.5 you put a curated slice of Gold into Lakebase. Now you put a simple app in front of it, so a depot manager gets a live console without ever touching the lakehouse. The app is a small Streamlit page: it reads the synced depot summary from Lakebase and shows each depot's headline numbers.
# MAGIC
# MAGIC This is a follow along walkthrough, and the app code is given to you in full. You do not write it. You read it to understand how an app authenticates to Lakebase and reads the serving table, then you deploy it through the workspace. There is nothing graded.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Before you start
# MAGIC
# MAGIC This walkthrough continues straight on from Lab 6.5 and has no bootstrap of its own. Everything it needs is workspace state that a notebook cell cannot restore: the `helios-ops` Lakebase project, its database registered in Unity Catalog as `helios_ops`, and `depot_ops_summary` synced into that catalog's `public` schema. The app reads the synced Postgres copy, not the lakehouse, so rebuilding the Gold here would prove nothing.
# MAGIC
# MAGIC If you are coming back in a new session, or your Lakebase project or synced table is no longer there, open Lab 6.5 and run it through again before you carry on. Its bootstrap rebuilds the Gold, the semantic layer and the `depot_ops_summary` serving table, and its workspace steps recreate the project, the catalog registration and the sync.
# MAGIC
# MAGIC You are ready to start here once the sync has reported success and reading `helios_ops.public.depot_ops_summary` returns the six depot rows, which is the last step of Lab 6.5.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The app, in three files
# MAGIC
# MAGIC A Databricks App is source code plus a manifest, deployed to the platform, which runs it as a service principal and puts it behind the workspace login. This one is three files:
# MAGIC
# MAGIC - `app.yaml`, the manifest: the start command and one environment value. When you attach the Lakebase database as an app resource, the platform injects the connection details (`PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGSSLMODE`) for you, so the only thing app.yaml sets is `ENDPOINT_NAME`, the compute's resource name, which the code needs to mint its token.
# MAGIC - `requirements.txt`, the Python dependencies: Streamlit, pandas, the Postgres driver and its pool, and the Databricks SDK.
# MAGIC - `app.py`, the console itself.
# MAGIC
# MAGIC The connection model is the idea worth understanding. The app holds no password. Attaching the database resource gives its service principal (identified by `DATABRICKS_CLIENT_ID`) a Postgres role and injects the host, port, database, user and SSL mode, but not a password. So for every new connection the app asks the platform for a short lived token (valid about an hour) with `generate_database_credential`, and uses that token as the password over SSL. A connection pool recycles connections before the token expires, so nothing has to refresh it by hand.
# MAGIC
# MAGIC The three files are printed below and also live in this repo under `06_lakebase_app/app`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### app.yaml
# MAGIC
# MAGIC ```yaml
# MAGIC # Databricks App manifest for the Helios Depot Operations Console.
# MAGIC #
# MAGIC # Databricks Apps runs Streamlit as a supported framework and sets the server port
# MAGIC # and address itself, so the command is just "streamlit run app.py". Do not pass
# MAGIC # --server.port here: the platform does not expand ${DATABRICKS_APP_PORT} in the
# MAGIC # command, so Streamlit would receive it literally and fail.
# MAGIC #
# MAGIC # Attach your Lakebase project as a Database resource (app Settings, Resources, Add
# MAGIC # resource, Database, pointing at your helios-ops project, the production branch and
# MAGIC # the databricks_postgres database, resource key "database"). That injects PGHOST,
# MAGIC # PGPORT, PGDATABASE, PGUSER and PGSSLMODE and gives the app service principal a
# MAGIC # Postgres role, so you do not set those here. The one value the resource does not
# MAGIC # inject is ENDPOINT_NAME, the compute resource name app.py uses to mint the
# MAGIC # connection token; get it from the branch's Computes tab (primary compute, Get ID,
# MAGIC # Copy resource name). If you named your project helios-ops the value below is right.
# MAGIC command:
# MAGIC   - streamlit
# MAGIC   - run
# MAGIC   - app.py
# MAGIC env:
# MAGIC   - name: ENDPOINT_NAME
# MAGIC     value: "projects/helios-ops/branches/production/endpoints/primary"
# MAGIC   - name: STREAMLIT_SERVER_ENABLE_CORS
# MAGIC     value: "false"
# MAGIC   - name: STREAMLIT_SERVER_ENABLE_XSRF_PROTECTION
# MAGIC     value: "false"
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### requirements.txt
# MAGIC
# MAGIC ```
# MAGIC # Python dependencies for the Depot Operations Console.
# MAGIC # databricks-sdk 0.81+ is required for the w.postgres credential helper.
# MAGIC # psycopg[binary,pool] is the PostgreSQL driver (version 3) plus its connection pool.
# MAGIC streamlit
# MAGIC pandas
# MAGIC psycopg[binary,pool]>=3.2
# MAGIC databricks-sdk>=0.81.0
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### app.py
# MAGIC
# MAGIC ```python
# MAGIC # Helios Depot Operations Console
# MAGIC #
# MAGIC # A small Streamlit app that serves the curated depot summary from Lakebase. It
# MAGIC # runs as the app service principal. Attaching the Lakebase database as an app
# MAGIC # resource injects PGHOST, PGPORT, PGDATABASE, PGUSER and PGSSLMODE and gives the
# MAGIC # service principal a Postgres role; no password is injected, so this code mints a
# MAGIC # short lived OAuth token per connection (using ENDPOINT_NAME from app.yaml) and
# MAGIC # uses it as the password. Every number shown comes from the depot_ops_summary
# MAGIC # table you synced in Lab 6.5, which was built from the Section 5 metric views, so
# MAGIC # the console agrees with Genie and the dashboards.
# MAGIC
# MAGIC import os
# MAGIC
# MAGIC import pandas as pd
# MAGIC import psycopg
# MAGIC import streamlit as st
# MAGIC from psycopg_pool import ConnectionPool
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC
# MAGIC # The synced Postgres table. Override with an app env var if you named it differently.
# MAGIC SUMMARY_TABLE = os.environ.get("SUMMARY_TABLE", "public.depot_ops_summary")
# MAGIC
# MAGIC st.set_page_config(page_title="Helios Depot Operations Console", page_icon="🛰️", layout="wide")
# MAGIC
# MAGIC workspace = WorkspaceClient()
# MAGIC
# MAGIC
# MAGIC class OAuthConnection(psycopg.Connection):
# MAGIC     # Mint a fresh Lakebase OAuth token for every new pooled connection. Tokens
# MAGIC     # last about an hour, so recycling connections (max_lifetime below) keeps the
# MAGIC     # pool authenticated without any manual refresh.
# MAGIC     @classmethod
# MAGIC     def connect(cls, conninfo="", **kwargs):
# MAGIC         token = workspace.postgres.generate_database_credential(
# MAGIC             endpoint=os.environ["ENDPOINT_NAME"]
# MAGIC         ).token
# MAGIC         kwargs["password"] = token
# MAGIC         return super().connect(conninfo, **kwargs)
# MAGIC
# MAGIC
# MAGIC @st.cache_resource
# MAGIC def get_pool():
# MAGIC     user = os.environ.get("PGUSER") or os.environ["DATABRICKS_CLIENT_ID"]
# MAGIC     conninfo = (
# MAGIC         f"host={os.environ['PGHOST']} port={os.environ.get('PGPORT', '5432')} "
# MAGIC         f"dbname={os.environ.get('PGDATABASE', 'databricks_postgres')} "
# MAGIC         f"user={user} sslmode={os.environ.get('PGSSLMODE', 'require')}"
# MAGIC     )
# MAGIC     return ConnectionPool(
# MAGIC         conninfo=conninfo,
# MAGIC         connection_class=OAuthConnection,
# MAGIC         min_size=1,
# MAGIC         max_size=5,
# MAGIC         max_lifetime=3000,
# MAGIC         open=True,
# MAGIC     )
# MAGIC
# MAGIC
# MAGIC def query(sql, params=None):
# MAGIC     with get_pool().connection() as conn:
# MAGIC         with conn.cursor() as cur:
# MAGIC             cur.execute(sql, params or ())
# MAGIC             columns = [c.name for c in cur.description]
# MAGIC             rows = cur.fetchall()
# MAGIC     return pd.DataFrame(rows, columns=columns)
# MAGIC
# MAGIC
# MAGIC st.title("🛰️ Helios Depot Operations Console")
# MAGIC st.caption("Live depot view served from Lakebase. Figures match the Section 5 semantic layer.")
# MAGIC
# MAGIC summary = query(f"SELECT * FROM {SUMMARY_TABLE} ORDER BY revenue DESC")
# MAGIC depot = st.selectbox("Depot", summary["depot"].tolist())
# MAGIC row = summary[summary["depot"] == depot].iloc[0]
# MAGIC
# MAGIC st.subheader(f"{row['depot']}  ({row['warehouse_id']}, {row['depot_body']}, {row['depot_region']})")
# MAGIC
# MAGIC top = st.columns(4)
# MAGIC top[0].metric("Revenue (CREDITS)", f"{float(row['revenue']):,.0f}")
# MAGIC top[1].metric("Gross margin rate", f"{float(row['gross_margin_rate']):.1%}")
# MAGIC top[2].metric("Orders", f"{int(row['orders']):,}")
# MAGIC top[3].metric("Cancellation rate", f"{float(row['cancellation_rate']):.2%}")
# MAGIC
# MAGIC bottom = st.columns(4)
# MAGIC bottom[0].metric("On time rate", f"{float(row['on_time_rate']):.1%}")
# MAGIC bottom[1].metric("Backordered orders", f"{int(row['backordered_orders']):,}")
# MAGIC bottom[2].metric("Stockout events", f"{int(row['stockout_events']):,}")
# MAGIC bottom[3].metric("Units shipped", f"{int(row['units_out']):,}")
# MAGIC
# MAGIC st.subheader("All depots")
# MAGIC st.dataframe(summary, use_container_width=True, hide_index=True)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create and deploy the app (workspace UI)
# MAGIC
# MAGIC 1. Put the three files in a workspace folder. Create a folder, for example `depot-ops-console` under your user home, and add `app.yaml`, `app.py` and `requirements.txt` with the versions below, or copy them from `06_lakebase_app/app` in this repo. The `streamlit run app.py` line in `app.yaml` is what makes this a Streamlit app.
# MAGIC 2. Create the app. Open the apps switcher, choose **Databricks Apps**, then **Create app**, and pick **Create a custom app** (the "Bring your code and resources to build an app from scratch" tile), not a template. Name it, for example `depot-ops-console` (lowercase with hyphens, up to 26 characters).
# MAGIC 3. Attach your Lakebase database as a resource. Open the app's **Settings**, and under **Resources** click **Add resource** and choose **Database**. Select your **helios-ops** project, the **production** branch, and the **databricks_postgres** database; leave the permission on **Can connect and create** and the resource key on **database**, then **Save**. This injects `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER` and `PGSSLMODE` into the app and gives its service principal a Postgres role, so you do not set those or grant connect access by hand.
# MAGIC 4. Check `ENDPOINT_NAME` in `app.yaml`, the one value the resource does not inject. In Lakebase open your project, the **production** branch, the **Computes** tab, and on the **primary** compute use **Get ID** then **Copy resource name**. If you named your project `helios-ops`, it is already `projects/helios-ops/branches/production/endpoints/primary`, the value in the file, so there is nothing to change.
# MAGIC 5. Click **Deploy**, select the folder from step 1 as the app's source code, and wait for the app status to reach **Running**. The first deploy installs the dependencies, so it takes a couple of minutes, and a brief 502 while Streamlit starts is normal.
# MAGIC
# MAGIC The same is scriptable with the Databricks CLI: `databricks sync . /Workspace/Users/<you>/depot-ops-console`, then `databricks apps deploy depot-ops-console --source-code-path /Workspace/Users/<you>/depot-ops-console`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## If the app cannot read the synced table
# MAGIC
# MAGIC Attaching the Lakebase database as an app resource gave the app's service principal a Postgres role with **connect** and **create** on the database, so it can log in. Reading the synced table is a separate **select** grant, because the sync owns that table rather than the app, so if the console shows a permission error, grant it once from the Lakebase SQL editor.
# MAGIC
# MAGIC 1. Open the app, go to its **Environment** tab, and copy the `DATABRICKS_CLIENT_ID` value (a UUID); this is the app's service principal.
# MAGIC 2. Open the SQL editor and attach it to Lakebase: from the compute drop down choose **More...**, then on the **Attach to an existing compute resource** dialog pick **Lakebase Postgres**, choose **Autoscaling**, select your project and branch, and click **Attach**.
# MAGIC 3. Run these, putting the client id inside the quotes:
# MAGIC
# MAGIC ```sql
# MAGIC GRANT USAGE ON SCHEMA public TO "<app_service_principal_client_id>";
# MAGIC GRANT SELECT ON public.depot_ops_summary TO "<app_service_principal_client_id>";
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Open the console
# MAGIC
# MAGIC Open the app from its page; the app has its own URL. You should see:
# MAGIC
# MAGIC - a depot picker, with the six depots;
# MAGIC - eight headline metrics for the chosen depot, matching the Section 5 numbers (Ares Depot shows revenue near 550.7 million CREDITS and a gross margin rate near 37.6 percent, pulled down by the batch three pricing incident; Europa Outpost shows the most stockout events, twelve);
# MAGIC - an all depots table at the bottom.
# MAGIC
# MAGIC Pick Ares Depot and then Europa Outpost and compare them. The console is serving live, low latency reads from Lakebase, and the whole path from the raw feeds to this screen is complete.
# MAGIC
# MAGIC Apps on Free Edition stop automatically after 24 hours to save resources; restart it from its page whenever you want it again.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What you saw
# MAGIC
# MAGIC You deployed a real operational app. It runs as its own service principal, authenticates to Lakebase with a short lived token, and reads the synced serving table to show each depot's operational picture in milliseconds. Nothing in the app aggregates raw data; it reads the small curated table you prepared, whose numbers come straight from the Section 5 metric views. That closes the loop the course set out to build: a data contract, an imperative pipeline, a declarative rebuild, a semantic layer, and now a live console, all over the same Helios Gold.