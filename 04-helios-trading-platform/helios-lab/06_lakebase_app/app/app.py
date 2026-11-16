# Helios Depot Operations Console
#
# A small Streamlit app that serves the curated depot summary from Lakebase. It
# runs as the app service principal. Attaching the Lakebase database as an app
# resource injects PGHOST, PGPORT, PGDATABASE, PGUSER and PGSSLMODE and gives the
# service principal a Postgres role; no password is injected, so this code mints a
# short lived OAuth token per connection (using ENDPOINT_NAME from app.yaml) and
# uses it as the password. Every number shown comes from the depot_ops_summary
# table you synced in Lab 6.5, which was built from the Section 5 metric views, so
# the console agrees with Genie and the dashboards.

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
