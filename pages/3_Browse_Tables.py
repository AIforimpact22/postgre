# pages/3_Browse_Tables.py
import streamlit as st
import pandas as pd
import psycopg2
from psycopg2 import sql
from db_utils import list_databases, get_conn, list_schemata_tables, get_table_columns_fq

st.title("Browse Tables")

# ── pick DB & table ────────────────────────────────────────────
db = st.selectbox("Database", list_databases())
if not db:
    st.stop()

tables = list_schemata_tables(db)
if not tables:
    st.info("No tables in this DB.")
    st.stop()

tbl_choice = st.selectbox("Table", tables)
schema, table = tbl_choice.split(".")

# Enter 0 to load the entire table
limit = st.number_input("Rows to display (0 = all)", 0, 1_000_000, 50)

# ── READ-ONLY query in AUTOCOMMIT mode (so no lingering locks) ─
with get_conn(db, auto_commit=True) as conn, conn.cursor() as cur:
    cur.execute("SET LOCAL lock_timeout = '1500ms';")      # fail fast if locked
    cur.execute("SET LOCAL statement_timeout = '30000ms';")

    try:
        if limit > 0:
            cur.execute(
                sql.SQL("SELECT * FROM {} LIMIT %s")
                   .format(sql.Identifier(schema, table)),
                (limit,),
            )
        else:
            cur.execute(
                sql.SQL("SELECT * FROM {}")
                   .format(sql.Identifier(schema, table))
            )

        rows = cur.fetchall()
        cols = get_table_columns_fq(db, schema, table)
        st.dataframe(pd.DataFrame(rows, columns=cols))

    except psycopg2.errors.LockNotAvailable:
        st.error("Table is locked by another session. Try again in a moment.")
    except psycopg2.errors.QueryCanceled:
        st.error("Query timed out.")
    except psycopg2.Error as e:
        st.error(f"Database error: {e.pgerror or e}")
