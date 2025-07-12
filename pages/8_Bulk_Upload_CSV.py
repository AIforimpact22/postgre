# pages/8_Bulk_Upload_CSV.py
"""
Bulk-upload a CSV into a chosen PostgreSQL table.
Assumes the CSV header line matches one-for-one with existing column names
(or a subset of them) in any order. Uses psycopg2 COPY for speed.
"""
import streamlit as st
import pandas as pd
import io
import psycopg2
from db_utils import list_databases, get_tables, get_columns, get_conn

st.title("📥 Bulk CSV Upload")

# ─────────────── select DB & table ───────────────
db = st.selectbox("Target database", list_databases())
if not db:
    st.stop()

tables = get_tables(db)
if not tables:
    st.warning("No tables in this database.")
    st.stop()

table = st.selectbox("Target table", tables)
if not table:
    st.stop()

tbl_cols = [c[0] for c in get_columns(db, table)]
st.write(f"Columns in **{table}**: {', '.join(tbl_cols)}")

# ─────────────── upload CSV ───────────────
uploaded = st.file_uploader("CSV file to upload", type=["csv"])
if uploaded is None:
    st.stop()

# read in pandas for preview / validation
try:
    df = pd.read_csv(uploaded)
except Exception as e:
    st.error(f"Could not read CSV: {e}")
    st.stop()

st.subheader("Preview (first 10 rows)")
st.dataframe(df.head(10))

# validate columns
missing = [c for c in df.columns if c not in tbl_cols]
if missing:
    st.error(f"These CSV columns do not exist in the table: {missing}")
    st.stop()

st.success(f"CSV has {len(df)} rows and {len(df.columns)} columns. Looks good!")

# ─────────────── insert section ───────────────
mode = st.radio("Insert mode", ["Append"], index=0, help="Only *append* is supported right now.")

if st.button("🚀 Upload to database"):
    with st.spinner("Copying data…"):
        buf = io.StringIO()
        # Write only the CSV data rows (no header)
        df.to_csv(buf, index=False, header=False)
        buf.seek(0)

        try:
            with get_conn(db) as conn:
                with conn.cursor() as cur:
                    cur.copy_from(
                        file=buf,
                        table=f'"{table}"',        # keep quotes for CamelCase names
                        columns=list(df.columns),
                        sep=","
                    )
                conn.commit()
            st.success(f"Inserted {len(df)} rows into **{db}.{table}** 🎉")
        except psycopg2.Error as e:
            st.error(f"Database error: {e.pgerror or e}")
        except Exception as e:
            st.error(e)
