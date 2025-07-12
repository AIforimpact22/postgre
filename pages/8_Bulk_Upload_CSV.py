# pages/8_Bulk_Upload_CSV.py
"""
Bulk-upload a CSV into any PostgreSQL table (fast COPY).
 • Lets you pick schema.table
 • Validates that CSV headers exist in the target table
 • Uses psycopg2.sql to quote identifiers properly (no more relation errors)
"""

import io
from typing import List

import pandas as pd
import psycopg2
from psycopg2 import sql
import streamlit as st

from db_utils import list_databases, get_conn   # make sure this exists

# ──────────────────────────────────── helpers ─────────────────────────────────
def list_schemata_tables(db: str) -> List[str]:
    """Return ['public.item', 'inventory.Item', …], excluding system schemas."""
    q = """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name;
    """
    with get_conn(db) as conn, conn.cursor() as cur:
        cur.execute(q)
        return [f"{s}.{t}" for s, t in cur.fetchall()]

def get_table_columns(db: str, schema: str, table: str) -> List[str]:
    q = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position;
    """
    with get_conn(db) as conn, conn.cursor() as cur:
        cur.execute(q, (schema, table))
        return [r[0] for r in cur.fetchall()]

# ──────────────────────────────────── UI ──────────────────────────────────────
st.title("📥 Bulk CSV Upload")

db = st.selectbox("Target database", list_databases())
if not db:
    st.stop()

tables = list_schemata_tables(db)
if not tables:
    st.warning("No user tables were found in this database.")
    st.stop()

tbl_choice = st.selectbox("Target table (schema.table)", tables)
if not tbl_choice:
    st.stop()

schema, tbl = tbl_choice.split(".", 1)
tbl_cols     = get_table_columns(db, schema, tbl)
st.write(f"Columns in **{tbl_choice}**: {', '.join(tbl_cols)}")

csv_file = st.file_uploader("CSV file to upload", type=["csv"])
if csv_file is None:
    st.stop()

try:
    df = pd.read_csv(csv_file)
except Exception as e:
    st.error(f"Could not read CSV: {e}")
    st.stop()

st.subheader("Preview (first 10 rows)")
st.dataframe(df.head(10))

missing = [c for c in df.columns if c not in tbl_cols]
if missing:
    st.error(f"These CSV columns do not exist in the table: {missing}")
    st.stop()

st.success(f"CSV looks good · {len(df)} rows · {len(df.columns)} columns")

# ────────────────────────────── COPY to PostgreSQL ───────────────────────────
st.radio("Insert mode", ["Append"], index=0)

if st.button("🚀 Upload to database"):

    with st.spinner("Copying data …"):
        # buffer with data rows only (no header)
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False)
        buf.seek(0)

        # Build safe COPY command:  COPY "schema"."table" ("c1","c2",…) FROM STDIN
        copy_sql = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv)").format(
            sql.Identifier(schema, tbl),
            sql.SQL(", ").join(map(sql.Identifier, df.columns)),
        )

        try:
            with get_conn(db) as conn, conn.cursor() as cur:
                cur.copy_expert(copy_sql, buf)
            st.success(f"Inserted {len(df)} rows into **{tbl_choice}** 🎉")

        except psycopg2.Error as e:
            st.error(f"Database error: {e.pgerror or e}")
        except Exception as e:
            st.error(str(e))
