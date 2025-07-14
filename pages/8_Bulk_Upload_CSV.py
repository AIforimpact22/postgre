# pages/8_Bulk_Upload_CSV.py
"""
Bulk-upload a CSV into any PostgreSQL table (fast COPY).

Key points
──────────
• Lets you pick schema.table
• Validates that CSV headers exist in the target table
• COPY runs in auto-commit mode → no lingering locks
• 1.5 s lock_timeout & 5 min statement_timeout inside COPY
• “Force-unlock & upload” cancels / terminates sessions that still hold a lock
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
    with get_conn(db, auto_commit=True) as conn, conn.cursor() as cur:
        cur.execute(q)
        return [f"{s}.{t}" for s, t in cur.fetchall()]

def get_table_columns(db: str, schema: str, table: str) -> List[str]:
    q = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position;
    """
    with get_conn(db, auto_commit=True) as conn, conn.cursor() as cur:
        cur.execute(q, (schema, table))
        return [r[0] for r in cur.fetchall()]

def force_unlock_table(db: str, schema: str, table: str) -> List[int]:
    """
    Cancel (then terminate, if needed) every session that still holds a lock
    on schema.table. Returns list of pids it touched.
    """
    pid_list: List[int] = []
    look_sql = """
        SELECT l.pid
        FROM pg_locks  l
        JOIN pg_class  c ON c.oid = l.relation
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s
          AND c.relname
