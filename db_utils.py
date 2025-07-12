# db_utils.py
"""
Shared database utilities for the AMAS Streamlit apps
─────────────────────────────────────────────────────
• get_conn(dbname=None, *, auto_commit=False)   → psycopg2 connection
    - Set auto_commit=True for one-shot maintenance commands
• list_databases()                              → [db1, db2, …]
• get_tables(db)  (public schema only)          → [tbl, …]
• get_columns(db, tbl)  (public schema only)    → [(col, type), …]
• New helpers with full schema support:
    - list_schemata_tables(db)                  → ['public.item', 'inventory.Item', …]
    - get_table_columns_fq(db, schema, tbl)     → [col, …]
• insert_row(db, tbl, dict) convenience
• valid_db  (regex)
"""

from __future__ import annotations

import psycopg2
import streamlit as st
import pandas as pd  # still used by some callers
import re
from typing import List

# ─────────────────────────────────── credentials ─────────────────────────────
pg = st.secrets["superuser"]  # role with CREATEDB

# ─────────────────────────────────── core connection ─────────────────────────
def get_conn(dbname: str | None = None, *, auto_commit: bool = False) -> psycopg2.extensions.connection:
    """
    Return a new psycopg2 connection.
    • dbname defaults to the super-user’s own DB (from secrets).
    • auto_commit=True → connection.autocommit set, so each statement commits
      immediately (handy for TRUNCATE / maintenance commands that must not
      leave locks open).
    """
    conn = psycopg2.connect(
        dbname=dbname or pg["dbname"],
        user=pg["user"],
        password=pg["password"],
        host=pg["host"],
        port=pg["port"],
    )
    if auto_commit:
        conn.autocommit = True
    return conn

# ─────────────────────────── convenience wrappers (public) ───────────────────
def list_databases() -> List[str]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT datname FROM pg_database "
            "WHERE datistemplate = false ORDER BY datname;"
        )
        return [r[0] for r in cur.fetchall()]

def get_schema(dbname: str):
    """Return (table_name, column_name, data_type) for the *public* schema."""
    with get_conn(dbname) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name, column_name, data_type
            FROM   information_schema.columns
            WHERE  table_schema = 'public'
            ORDER  BY table_name, ordinal_position;
            """
        )
        return cur.fetchall()

def get_tables(dbname: str) -> List[str]:
    """List tables in the *public* schema only (legacy helper)."""
    with get_conn(dbname) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
            """
        )
        return [r[0] for r in cur.fetchall()]

def get_columns(dbname: str, table: str):
    """Get columns for a table in the *public* schema (legacy helper)."""
    with get_conn(dbname) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER  BY ordinal_position;
            """,
            (table,),
        )
        return cur.fetchall()

def insert_row(dbname: str, table: str, data: dict):
    """Simple INSERT helper for tables in the public schema."""
    with get_conn(dbname, auto_commit=True) as conn, conn.cursor() as cur:
        cols = ", ".join(f'"{c}"' for c in data.keys())
        placeholders = ", ".join(["%s"] * len(data))
        cur.execute(
            f'INSERT INTO "{table}" ({cols}) VALUES ({placeholders})',
            list(data.values()),
        )

# ─────────────────────────────── schema-aware helpers ────────────────────────
def list_schemata_tables(dbname: str) -> List[str]:
    """
    Return every user table as 'schema.table', excluding system schemas.
    Useful for pages that need to browse across multiple schemas.
    """
    q = """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name;
    """
    with get_conn(dbname) as conn, conn.cursor() as cur:
        cur.execute(q)
        return [f"{s}.{t}" for s, t in cur.fetchall()]

def get_table_columns_fq(dbname: str, schema: str, table: str) -> List[str]:
    """Return column names for a fully-qualified table."""
    q = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position;
    """
    with get_conn(dbname) as conn, conn.cursor() as cur:
        cur.execute(q, (schema, table))
        return [r[0] for r in cur.fetchall()]

# ───────────────────────────── handy validators ──────────────────────────────
valid_db = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$").fullmatch
