# pages/2_Edit_Database.py
import streamlit as st
import pandas as pd
import psycopg2
from typing import List
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

# ─────────────────────── helpers ───────────────────────
def split_sql_statements(sql_text: str) -> List[str]:
    """
    Split SQL into individual statements.
    - Prefer sqlparse if available (handles strings/quotes).
    - Fallback: strip '--' line comments and split by ';'.
    """
    try:
        import sqlparse  # type: ignore
        # Filter out empty/whitespace statements
        statements = [
            str(stmt).strip()
            for stmt in sqlparse.parse(sql_text)
            if str(stmt).strip().strip(';')
        ]
        # sqlparse.parse doesn't preserve trailing semicolons; ensure we don't emit empties
        return [s.rstrip(';').strip() for s in statements if s.rstrip(';').strip()]
    except Exception:
        # Fallback: remove -- line comments, keep everything else
        lines = []
        for line in sql_text.splitlines():
            # Remove inline "-- ..." comments (not perfect but safe enough if no strings with "--")
            if '--' in line:
                line = line.split('--', 1)[0]
            lines.append(line)
        cleaned = '\n'.join(lines)
        parts = cleaned.split(';')
        return [p.strip() for p in parts if p.strip()]

def run_one_statement(cur, sql):
    """Execute one statement and return ('result', dataframe) or ('ok', rowcount/message)."""
    # Fail fast if another session holds a lock, and cap statement runtime
    cur.execute("SET LOCAL lock_timeout = '1500ms';")
    cur.execute("SET LOCAL statement_timeout = '30000ms';")

    cur.execute(sql)

    if cur.description:  # SELECT or anything returning rows
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description]
        return ("result", pd.DataFrame(rows, columns=cols))
    else:
        # Non-SELECT (DDL/DML)
        # rowcount is -1 for statements where it doesn't apply
        rc = cur.rowcount
        msg = cur.statusmessage or "Command executed."
        return ("ok", f"{msg}" + ("" if rc in (-1, None) else f" • rowcount={rc}"))

# ─────────────────────── 2. Multi-statement SQL runner ────────────
st.subheader("Run arbitrary SQL")
sql_txt = st.text_area(
    "SQL to execute inside this DB (multiple statements supported)",
    height=180,
    placeholder="Paste one or more SQL statements separated by semicolons…",
)

if st.button("Run SQL"):
    stmts = split_sql_statements(sql_txt or "")
    if not stmts:
        st.warning("No SQL statements detected.")
        st.stop()

    with st.spinner(f"Executing {len(stmts)} statement(s)…"):
        with get_conn(db, auto_commit=True) as conn:
            for idx, stmt in enumerate(stmts, start=1):
                pretty_header = f"Statement {idx}"
                with st.container(border=True):
                    st.markdown(f"**{pretty_header}**")
                    st.code(stmt, language="sql")

                    try:
                        with conn.cursor() as cur:
                            kind, payload = run_one_statement(cur, stmt)
                    except psycopg2.errors.LockNotAvailable:
                        st.error("Command aborted: table is locked by another session.")
                        continue
                    except psycopg2.errors.QueryCanceled:
                        st.error("Command timed out (statement or lock timeout).")
                        continue
                    except Exception as e:
                        st.error(f"{type(e).__name__}: {e}")
                        continue

                    if kind == "result":
                        st.dataframe(payload, use_container_width=True)
                        st.caption(f"Returned {len(payload)} row(s).")
                    else:
                        st.success(payload)
