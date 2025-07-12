# pages/2_Edit_Database.py
import streamlit as st
import pandas as pd
import psycopg2
from db_utils import (
    list_databases,
    get_schema,
    get_conn,
)

st.title("Edit / Inspect Database")

db = st.selectbox("Choose a database", list_databases())
if not db:
    st.stop()

# ─────────────────────── 1. Schema overview ───────────────────────
st.subheader("Schema overview")

# get_schema() still works, but we want to avoid any lingering locks,
# so we re-implement the call here with auto_commit=True
with get_conn(db, auto_commit=True) as conn, conn.cursor() as cur:
    cur.execute(
        """
        SELECT table_name, column_name, data_type
        FROM   information_schema.columns
        WHERE  table_schema = 'public'
        ORDER  BY table_name, ordinal_position;
        """
    )
    schema_rows = cur.fetchall()

if schema_rows:
    by_table = {}
    for t, c, d in schema_rows:
        by_table.setdefault(t, []).append(f"{c} ({d})")
    for t, cols in by_table.items():
        st.markdown(f"**{t}**")
        st.write(", ".join(cols))
else:
    st.info("No tables found.")

# ─────────────────────── 2. Ad-hoc SQL runner ─────────────────────
st.subheader("Run arbitrary SQL")
sql_txt = st.text_area("SQL to execute inside this DB", height=140)

if st.button("Run SQL"):
    with st.spinner("Executing…"):
        try:
            # auto_commit=True → every statement finishes (and commits) on exit
            with get_conn(db, auto_commit=True) as conn, conn.cursor() as cur:
                # Fail fast if another session holds a lock on a table we need
                cur.execute("SET LOCAL lock_timeout = '1500ms';")
                cur.execute("SET LOCAL statement_timeout = '30000ms';")

                cur.execute(sql_txt)

                if cur.description:        # returned a result set
                    rows = cur.fetchall()
                    cols = [c[0] for c in cur.description]
                    st.dataframe(pd.DataFrame(rows, columns=cols))
                else:
                    st.success("Command executed.")

        except psycopg2.errors.LockNotAvailable:
            st.error("Command aborted: table is locked by another session.")
        except psycopg2.errors.QueryCanceled:
            st.error("Command timed out (statement or lock timeout).")
        except Exception as e:
            st.error(e)
