# pages/8_Bulk_Upload_CSV.py
"""
Bulk-upload a CSV into any PostgreSQL table (fast COPY).

Key points
──────────
• Lets you pick schema.table
• Validates that CSV headers exist in the target table
• Runs COPY in auto-commit mode → no lingering locks
• New: “Force-unlock & upload” button cancels / terminates sessions that
  still hold an ACCESS EXCLUSIVE lock on the table (e.g. from a forgotten
  TRUNCATE or failed COPY) before retrying the upload.
"""

from __future__ import annotations

import io
from typing import List

import pandas as pd
import psycopg2
from psycopg2 import sql
import streamlit as st

from db_utils import list_databases, get_conn

# ───────────────────────────── helpers ──────────────────────────────
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

def force_unlock_table(db: str, schema: str, table: str) -> List[int]:
    """
    Cancel (then terminate, if needed) every session that still holds a lock
    on schema.table. Returns list of pids it acted on.
    """
    pid_list: List[int] = []
    look_sql = """
        SELECT l.pid
        FROM pg_locks  l
        JOIN pg_class  c ON c.oid = l.relation
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s
          AND c.relname = %s
          AND l.pid <> pg_backend_pid();
    """
    with get_conn(db, auto_commit=True) as conn, conn.cursor() as cur:
        cur.execute(look_sql, (schema, table))
        pids = [r[0] for r in cur.fetchall()]

        for pid in pids:
            cur.execute("SELECT pg_cancel_backend(%s);", (pid,))
            cur.execute(look_sql, (schema, table))
            still_locked = cur.fetchone() is not None
            if still_locked:
                cur.execute("SELECT pg_terminate_backend(%s);", (pid,))
            pid_list.append(pid)
    return pid_list

def copy_csv(conn, df: pd.DataFrame, schema: str, tbl: str):
    """Execute COPY ... FROM STDIN using the provided connection (autocommit)."""
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)

    copy_sql = sql.SQL(
        "COPY {} ({}) FROM STDIN WITH (FORMAT csv)"
    ).format(
        sql.Identifier(schema, tbl),
        sql.SQL(", ").join(map(sql.Identifier, df.columns)),
    )

    with conn.cursor() as cur:
        cur.copy_expert(copy_sql, buf)

# ─────────────────────────────── UI ────────────────────────────────
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
tbl_cols = get_table_columns(db, schema, tbl)
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

# ──────────────────────── Upload buttons ──────────────────────────
col1, col2 = st.columns(2)
with col1:
    run_upload = st.button("🚀 Upload")
with col2:
    force_upload = st.button("🛠 Force-unlock & upload")

def do_upload(force: bool = False):
    try:
        if force:
            pids = force_unlock_table(db, schema, tbl)
            st.info(f"Cancelled/terminated blocking pids: {pids or 'none found'}")

        with st.spinner("Copying data …"):
            with get_conn(db, auto_commit=True) as conn:
                copy_csv(conn, df, schema, tbl)
        st.success(f"Inserted {len(df)} rows into **{tbl_choice}** 🎉")

    except psycopg2.errors.LockNotAvailable:
        st.error("Table is locked by another session. Try 'Force-unlock & upload'.")
    except psycopg2.Error as e:
        st.error(f"Database error: {e.pgerror or e}")
    except Exception as e:
        st.error(str(e))

if run_upload:
    do_upload(force=False)
elif force_upload:
    do_upload(force=True)
