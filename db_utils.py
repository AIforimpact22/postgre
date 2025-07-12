# db_utils.py
import streamlit as st
import psycopg2
import pandas as pd
import re

pg = st.secrets["superuser"]            # super‑user credentials (role with CREATEDB)

def get_conn(dbname: str | None = None):
    """Return a new psycopg2 connection; dbname defaults to the superuser’s DB."""
    return psycopg2.connect(
        dbname=dbname or pg["dbname"],
        user=pg["user"],
        password=pg["password"],
        host=pg["host"],
        port=pg["port"],
    )

# ───────── convenience wrappers ─────────
def list_databases():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT datname FROM pg_database "
            "WHERE datistemplate = false ORDER BY datname;"
        )
        return [r[0] for r in cur.fetchall()]

def get_schema(dbname):
    with get_conn(dbname) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT table_name, column_name, data_type
            FROM   information_schema.columns
            WHERE  table_schema = 'public'
            ORDER  BY table_name, ordinal_position;
            """
        )
        return cur.fetchall()

def get_tables(dbname):
    with get_conn(dbname) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='public' AND table_type='BASE TABLE';
            """
        )
        return [r[0] for r in cur.fetchall()]

def get_columns(dbname, table):
    with get_conn(dbname) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=%s
            ORDER  BY ordinal_position;
            """,
            (table,),
        )
        return cur.fetchall()

def insert_row(dbname, table, data: dict):
    with get_conn(dbname) as conn:
        cur = conn.cursor()
        cols = ", ".join(data.keys())
        placeholders = ", ".join(["%s"] * len(data))
        cur.execute(f'INSERT INTO "{table}" ({cols}) VALUES ({placeholders})', list(data.values()))
        conn.commit()

# ───────── handy validators ─────────
valid_db = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$").fullmatch
