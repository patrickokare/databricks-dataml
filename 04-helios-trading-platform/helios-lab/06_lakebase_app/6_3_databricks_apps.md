# Helios Trading Corporation: The Depot Operations Console

The serving side of Section 6 puts a curated depot summary into Lakebase; this document covers the app that reads it. The Depot Operations Console is a small Streamlit page the depot operations team wrote: a depot picker, the chosen depot's headline numbers, and a table of all six depots, every figure read live from the synced `depot_ops_summary` table. That table is curated and synced in the labs; as far as the app is concerned it is simply a Postgres table to read. The sections below explain what a Databricks App is, how this one authenticates to Lakebase, and then each of its three source files. The files themselves ship in this repo under `06_lakebase_app/app`.

---

## Databricks Apps

A Databricks App is source code plus a manifest, deployed into the workspace, which runs it as a hosted service. The platform installs the dependencies, starts the process the manifest names, and gives the app its own URL, so there is no server to provision or patch. The app runs as a service principal, an identity of its own, which means it carries its own grants rather than borrowing any user's, and opening the URL requires a workspace login, so the console is only reachable by people who can sign in. Python frameworks such as Streamlit, Dash, Gradio and Flask are supported directly, along with Node.js, and the compute is serverless, billed while the app runs.

---

## How the app connects to Lakebase

There is no password anywhere in this codebase, and that is the part of the design to understand before reading the files.

When the app is deployed, its Lakebase database is attached to it as a resource. That attachment does two things: it gives the app's service principal a Postgres role in the database, and it injects the connection details into the app's environment as `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER` and `PGSSLMODE`. What it deliberately does not inject is a password. Instead, each time the app opens a new connection it asks the platform for a short lived OAuth token with `generate_database_credential`, valid for about an hour, and presents that token as the Postgres password over SSL. A connection pool recycles its connections before their tokens expire, so nothing ever refreshes a credential by hand, and there is no secret to store, rotate or leak.

One value is not injected by the resource: `ENDPOINT_NAME`, the resource name of the branch's compute endpoint, which the token request needs. The manifest sets it.

---

## app.yaml

The manifest is the start command and the environment values the platform does not inject.

```yaml
command:
  - streamlit
  - run
  - app.py
env:
  - name: ENDPOINT_NAME
    value: "projects/helios-ops/branches/production/endpoints/primary"
  - name: STREAMLIT_SERVER_ENABLE_CORS
    value: "false"
  - name: STREAMLIT_SERVER_ENABLE_XSRF_PROTECTION
    value: "false"
```

Streamlit is a supported framework, and the platform sets the server port and address itself, so the command is just `streamlit run app.py` with no port flags. `ENDPOINT_NAME` is the compute endpoint's resource name described above. The two Streamlit flags are off because the workspace serves the app through its own authenticated proxy, which handles the login, and Streamlit's built in cross origin and request forgery checks would reject requests arriving through it.

---

## requirements.txt

The dependencies the platform installs at deploy time.

```
streamlit
pandas
psycopg[binary,pool]>=3.2
databricks-sdk>=0.81.0
```

Streamlit renders the page and pandas holds the query results as DataFrames. `psycopg` is the Postgres driver, version 3, with the `binary` extra so no compiler is needed and the `pool` extra for its connection pool. The Databricks SDK provides `generate_database_credential`, the token call, which arrived in version 0.81, so the version floor is not decorative.

---

## app.py

The console itself, in three parts: the authenticated connection, a small query helper, and the page.

```python
import os

import pandas as pd
import psycopg
import streamlit as st
from psycopg_pool import ConnectionPool
from databricks.sdk import WorkspaceClient

# The synced Postgres table. Override with an app env var if you named it differently.
SUMMARY_TABLE = os.environ.get("SUMMARY_TABLE", "public.depot_ops_summary")

st.set_page_config(page_title="Helios Depot Operations Console", page_icon="🛰️", layout="wide")

workspace = WorkspaceClient()


class OAuthConnection(psycopg.Connection):
    # Mint a fresh Lakebase OAuth token for every new pooled connection. Tokens
    # last about an hour, so recycling connections (max_lifetime below) keeps the
    # pool authenticated without any manual refresh.
    @classmethod
    def connect(cls, conninfo="", **kwargs):
        token = workspace.postgres.generate_database_credential(
            endpoint=os.environ["ENDPOINT_NAME"]
        ).token
        kwargs["password"] = token
        return super().connect(conninfo, **kwargs)


@st.cache_resource
def get_pool():
    user = os.environ.get("PGUSER") or os.environ["DATABRICKS_CLIENT_ID"]
    conninfo = (
        f"host={os.environ['PGHOST']} port={os.environ.get('PGPORT', '5432')} "
        f"dbname={os.environ.get('PGDATABASE', 'databricks_postgres')} "
        f"user={user} sslmode={os.environ.get('PGSSLMODE', 'require')}"
    )
    return ConnectionPool(
        conninfo=conninfo,
        connection_class=OAuthConnection,
        min_size=1,
        max_size=5,
        max_lifetime=3000,
        open=True,
    )


def query(sql, params=None):
    with get_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            columns = [c.name for c in cur.description]
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=columns)


st.title("🛰️ Helios Depot Operations Console")
st.caption("Live depot view served from Lakebase. Figures match the Section 5 semantic layer.")

summary = query(f"SELECT * FROM {SUMMARY_TABLE} ORDER BY revenue DESC")
depot = st.selectbox("Depot", summary["depot"].tolist())
row = summary[summary["depot"] == depot].iloc[0]

st.subheader(f"{row['depot']}  ({row['warehouse_id']}, {row['depot_body']}, {row['depot_region']})")

top = st.columns(4)
top[0].metric("Revenue (CREDITS)", f"{float(row['revenue']):,.0f}")
top[1].metric("Gross margin rate", f"{float(row['gross_margin_rate']):.1%}")
top[2].metric("Orders", f"{int(row['orders']):,}")
top[3].metric("Cancellation rate", f"{float(row['cancellation_rate']):.2%}")

bottom = st.columns(4)
bottom[0].metric("On time rate", f"{float(row['on_time_rate']):.1%}")
bottom[1].metric("Backordered orders", f"{int(row['backordered_orders']):,}")
bottom[2].metric("Stockout events", f"{int(row['stockout_events']):,}")
bottom[3].metric("Units shipped", f"{int(row['units_out']):,}")

st.subheader("All depots")
st.dataframe(summary, use_container_width=True, hide_index=True)
```

The connection is the `OAuthConnection` class. It extends the ordinary psycopg connection with one change: whenever the pool opens a new connection, `connect` first calls `generate_database_credential` with the endpoint name from the environment and passes the returned token as the password. `get_pool` builds the connection string entirely from the injected `PG*` variables, with `PGUSER` falling back to `DATABRICKS_CLIENT_ID`, the service principal's own id. The pool keeps between one and five connections and retires each one after 3,000 seconds, comfortably inside the roughly one hour token lifetime, which is what makes the manual-refresh-free design hold. The `st.cache_resource` decorator means the pool is built once per app process rather than once per page rerun.

The `query` helper borrows a connection from the pool, runs the SQL, and returns the rows as a pandas DataFrame with the cursor's column names.

The page is the last third. One query reads the whole summary table, all six depot rows, ordered by revenue. A select box picks the depot, eight `st.metric` tiles show its headline numbers, and the full table renders at the bottom. Nothing on the page aggregates anything: every figure was computed in the lakehouse, synced into Lakebase at exactly this grain, and because the serving table was built from the Section 5 metric views, the console, Genie and the dashboards all report the same numbers.

---

## References

- [Databricks Apps](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/)
- [Configure app execution with app.yaml](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/app-runtime)
- [Using Lakebase with Databricks Apps](https://docs.databricks.com/aws/en/oltp/projects/databricks-apps)
- [Connect a custom Databricks app to Lakebase](https://docs.databricks.com/aws/en/oltp/projects/tutorial-databricks-apps-autoscaling)
