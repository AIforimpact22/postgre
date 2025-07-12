# pages/8_Bulk_Upload_CSV.py
"""
Bulk-upload a CSV into any PostgreSQL table (fast COPY).
✓ Schema-aware picker: shows schema.table
✓ Validates headers ⟷ table columns
"""

import io
from typing import List

import pandas as pd
import psycopg2
import streamlit as st
from db_utils import list_databases, get_conn   # make sure this exists

# ────────────────────────────────────────────────────────────────
# CONFIG  (optional search_path override) ------------------------
USE_SEARCH_PATH = False
SEARCH_PATH     = "public"
# ────────────────────────────────────────────────────────────────

st.title("📥 Bulk CSV Upload")

# helper ­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­
def list_schemata_tables(db: str) -> List[str]:
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

# 1️⃣  pick DB & table ­­­­­­­­­­­­­­­­­­
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

schema, tbl   = tbl_choice.split(".", 1)
tbl_cols      = get_table_columns(db, schema, tbl)
st.write(f"Columns in **{tbl_choice}**: {', '.join(tbl_cols)}")

# 2️⃣  upload & preview CSV ­­­­­­­­­­­
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

# 3️⃣  insert (append-only) ­­­­­­­­­­­
st.radio("Insert mode", ["Append"], index=0)

if st.button("🚀 Upload to database"):

    with st.spinner("Copying data …"):
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False)
        buf.seek(0)

        table_ident = f"{schema}.{tbl}"   # ← UNQUOTED

        try:
            with get_conn(db) as conn, conn.cursor() as cur:
                if USE_SEARCH_PATH:
                    cur.execute(f"SET search_path TO {SEARCH_PATH};")

                cur.copy_from(
                    file=buf,
                    table=table_ident,
                    columns=list(df.columns),
                    sep=","
                )
            st.success(f"Inserted {len(df)} rows into **{tbl_choice}** 🎉")

        except psycopg2.Error as e:
            st.error(f"Database error: {e.pgerror or e}")
        except Exception as e:
            st.error(str(e))
