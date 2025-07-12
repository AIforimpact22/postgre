# pages/2_Edit_Database.py
import streamlit as st
import pandas as pd
from db_utils import list_databases, get_schema, get_conn

st.title("Edit / Inspect Database")

db = st.selectbox("Choose a database", list_databases())
if not db:
    st.stop()

# ─── schema overview ──────────────────
st.subheader("Schema overview")
schema = get_schema(db)
if schema:
    by_table = {}
    for t, c, d in schema:
        by_table.setdefault(t, []).append(f"{c} ({d})")
    for t, cols in by_table.items():
        st.markdown(f"**{t}**")
        st.write(", ".join(cols))
else:
    st.info("No tables found.")

# ─── SQL runner ───────────────────────
st.subheader("Run arbitrary SQL")
sql = st.text_area("SQL to execute inside this DB", height=140)
if st.button("Run SQL"):
    with st.spinner("Executing…"):
        try:
            with get_conn(db) as conn, conn.cursor() as cur:
                cur.execute(sql)
                if cur.description:
                    rows = cur.fetchall()
                    cols = [c[0] for c in cur.description]
                    st.dataframe(pd.DataFrame(rows, columns=cols))
                else:
                    conn.commit()
                    st.success("Command executed.")
        except Exception as e:
            st.error(e)
