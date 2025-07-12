# pages/3_Browse_Tables.py  (full file)
import streamlit as st
import pandas as pd
import psycopg2
from psycopg2 import sql
from db_utils import list_databases, get_conn

st.title("Browse Tables")

# ----- helper to list schema.table -----
def list_schemata_tables(db):
    q = """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name"""
    with get_conn(db) as conn, conn.cursor() as cur:
        cur.execute(q)
        return [f"{s}.{t}" for s, t in cur.fetchall()]

def get_cols(db, schema, table):
    q = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position"""
    with get_conn(db) as conn, conn.cursor() as cur:
        cur.execute(q, (schema, table))
        return [r[0] for r in cur.fetchall()]

# ----- UI -----
db = st.selectbox("Database", list_databases())
if not db:
    st.stop()

tables = list_schemata_tables(db)
if not tables:
    st.info("No tables in this DB.")
    st.stop()

tbl_choice = st.selectbox("Table (schema.table)", tables)
schema, table = tbl_choice.split(".", 1)

limit = st.number_input("Rows to display", 1, 1000, 50)

# ----- guarded query -----
with get_conn(db) as conn, conn.cursor() as cur:
    # TIMEOUTS (adjust to taste)
    cur.execute("SET LOCAL lock_timeout = '1500ms';")      # wait ≤1.5 s for lock
    cur.execute("SET LOCAL statement_timeout = '30000ms';")  # whole query ≤30 s
    try:
        cur.execute(
            sql.SQL("SELECT * FROM {} LIMIT %s").format(
                sql.Identifier(schema, table)
            ),
            (limit,),
        )
        rows = cur.fetchall()
        st.dataframe(pd.DataFrame(rows, columns=get_cols(db, schema, table)))
    except psycopg2.errors.LockNotAvailable:
        st.error("Table is locked by another session (likely a TRUNCATE or COPY).")
    except psycopg2.errors.QueryCanceled:
        st.error("Query timed out.")
    except psycopg2.Error as e:
        st.error(f"Database error: {e.pgerror or e}")
