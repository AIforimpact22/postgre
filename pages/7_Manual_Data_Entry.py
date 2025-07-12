# pages/7_Manual_Data_Entry.py
import streamlit as st
from db_utils import (
    list_databases,
    get_tables,
    get_columns,
    insert_row,
)

st.title("Manual Data Entry")

db = st.selectbox("Database", list_databases())
if not db:
    st.stop()

tables = get_tables(db)
table = st.selectbox("Table", tables)
if not table:
    st.stop()

columns = get_columns(db, table)
st.write(f"Columns in **{table}**: {[c[0] for c in columns]}")

with st.form("entry"):
    data = {}
    for name, dtype in columns:
        # skip obvious serial/identity columns
        if dtype.lower() in ("integer", "bigint") and name.endswith("id"):
            continue
        data[name] = st.text_input(f"{name} ({dtype})")
    if st.form_submit_button("Insert row"):
        # basic type casting
        for name, dtype in columns:
            if data.get(name) == "":
                data[name] = None
            elif dtype in ("integer", "bigint"):
                data[name] = int(data[name])
            elif dtype == "double precision":
                data[name] = float(data[name])
        cleaned = {k: v for k, v in data.items() if v is not None}
        try:
            insert_row(db, table, cleaned)
            st.success(f"Inserted: {cleaned}")
        except Exception as e:
            st.error(e)
