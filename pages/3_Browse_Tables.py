# pages/3_Browse_Tables.py
import streamlit as st
import pandas as pd
from db_utils import list_databases, get_tables, get_columns, get_conn

st.title("Browse Tables")

db = st.selectbox("Database", list_databases())
if not db:
    st.stop()

tables = get_tables(db)
if not tables:
    st.info("No tables in this DB.")
    st.stop()

table = st.selectbox("Table", tables)
cols = [c[0] for c in get_columns(db, table)]
limit = st.number_input("Rows to display", min_value=1, max_value=1000, value=50)

with get_conn(db) as conn, conn.cursor() as cur:
    cur.execute(f'SELECT * FROM "{table}" LIMIT %s;', (limit,))
    rows = cur.fetchall()

st.dataframe(pd.DataFrame(rows, columns=cols))
