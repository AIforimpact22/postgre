# pages/2_Edit_Database.py
import time
from typing import List, Tuple, Optional

import pandas as pd
import psycopg2
import streamlit as st

from db_utils import (
    list_databases,
    get_schema,   # kept for compatibility (not used directly here)
    get_conn,
)

# ───────────────────────────── UI: page + DB select ─────────────────────────────
st.title("Edit / Inspect Database")

db = st.selectbox("Choose a database", list_databases())
if not db:
    st.stop()

st.caption("Tip: Use single-transaction mode to run several statements atomically, "
           "or autocommit to run each statement independently.")

# ───────────────────────────── Schema overview ─────────────────────────────
st.subheader("Schema overview")

def get_available_schemas(conn) -> List[str]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog','information_schema')
            ORDER BY schema_name;
        """)
        return [r[0] for r in cur.fetchall()]

with get_conn(db, auto_commit=True) as conn:
    schemas = get_available_schemas(conn)

schema = st.selectbox("Schema", schemas or ["public"], index=(schemas or ["public"]).index("public") if "public" in (schemas or []) else 0)

with get_conn(db, auto_commit=True) as conn, conn.cursor() as cur:
    cur.execute(
        """
        SELECT table_name, column_name, data_type
        FROM   information_schema.columns
        WHERE  table_schema = %s
        ORDER  BY table_name, ordinal_position;
        """,
        (schema,),
    )
    schema_rows = cur.fetchall()

if schema_rows:
    by_table: dict[str, list[str]] = {}
    for t, c, d in schema_rows:
        by_table.setdefault(t, []).append(f"{c} ({d})")
    for t, cols in by_table.items():
        st.markdown(f"**{t}**")
        st.write(", ".join(cols))
else:
    st.info("No tables found in this schema.")

# ───────────────────────────── Helpers ─────────────────────────────

def split_sql_statements(sql_text: str) -> List[str]:
    """
    Split SQL into individual statements using sqlparse (required).
    Handles semicolons, quoted strings, DO blocks, dollar-quoting, etc.
    """
    try:
        import sqlparse  # type: ignore
    except Exception:
        st.error(
            "Multi-statement execution requires `sqlparse`. "
            "Install it in your environment (e.g., `pip install sqlparse`)."
        )
        st.stop()

    statements = [
        str(stmt).strip()
        for stmt in sqlparse.parse(sql_text or "")
        if str(stmt).strip().strip(";")
    ]
    # Remove trailing semicolons that sqlparse doesn't normalize consistently
    return [s.rstrip(";").strip() for s in statements if s.rstrip(";").strip()]

def _set_timeouts(cur, lock_timeout_ms: int, statement_timeout_ms: int, in_txn: bool):
    """
    Apply timeouts. Use SET LOCAL inside a transaction, otherwise SET and RESET.
    Returns a boolean indicating whether RESET is required on exit.
    """
    if in_txn:
        cur.execute(f"SET LOCAL lock_timeout = '{int(lock_timeout_ms)}ms';")
        cur.execute(f"SET LOCAL statement_timeout = '{int(statement_timeout_ms)}ms';")
        return False
    else:
        cur.execute(f"SET lock_timeout = '{int(lock_timeout_ms)}ms';")
        cur.execute(f"SET statement_timeout = '{int(statement_timeout_ms)}ms';")
        return True

def _reset_timeouts(cur):
    cur.execute("RESET lock_timeout;")
    cur.execute("RESET statement_timeout;")

def run_one_statement(
    conn,
    sql: str,
    lock_timeout_ms: int,
    statement_timeout_ms: int,
    max_rows: int,
    explain: bool,
    in_txn: bool,
) -> Tuple[str, object, float]:
    """
    Execute one statement and return (kind, payload, elapsed_seconds).
    kind: "result" -> payload is DataFrame, "ok" -> payload is status string
    """
    to_exec = sql if not explain else f"EXPLAIN (ANALYZE, BUFFERS, VERBOSE) {sql}"

    start = time.perf_counter()
    with conn.cursor() as cur:
        needs_reset = _set_timeouts(cur, lock_timeout_ms, statement_timeout_ms, in_txn)
        # For long-lived transactions, also guard against lingering idles:
        if in_txn:
            cur.execute("SET LOCAL idle_in_transaction_session_timeout = '15000ms';")

        try:
            cur.execute(to_exec)

            if cur.description:  # rows returned
                rows = cur.fetchall()
                cols = [c[0] for c in cur.description]
                if max_rows is not None and max_rows > 0 and len(rows) > max_rows:
                    rows = rows[:max_rows]
                payload = pd.DataFrame(rows, columns=cols)
                kind = "result"
            else:
                rc = cur.rowcount
                msg = cur.statusmessage or "Command executed."
                payload = f"{msg}" + ("" if rc in (-1, None) else f" • rowcount={rc}")
                kind = "ok"

        finally:
            if needs_reset:
                _reset_timeouts(cur)

    elapsed = time.perf_counter() - start
    return (kind, payload, elapsed)

# ───────────────────────────── SQL Runner UI ─────────────────────────────
st.subheader("Run arbitrary SQL")

with st.expander("Execution settings", expanded=True):
    col1, col2, col3 = st.columns(3)
    with col1:
        single_txn = st.checkbox(
            "Run all statements in a single transaction (rollback on first error)",
            value=False,
            help="If unchecked, each statement runs & commits independently (autocommit).",
        )
    with col2:
        lock_timeout_ms = st.number_input(
            "Lock timeout (ms)", min_value=0, max_value=60000, value=2000, step=250,
            help="Fail fast if another session holds a conflicting lock.",
        )
    with col3:
        statement_timeout_ms = st.number_input(
            "Statement timeout (ms)", min_value=0, max_value=10_000_000, value=30_000, step=500,
            help="Abort statements that run too long.",
        )
    col4, col5 = st.columns(2)
    with col4:
        max_rows = st.number_input(
            "Max rows to display", min_value=100, max_value=1_000_000, value=5000, step=100,
            help="Limits result-set size shown in the UI.",
        )
    with col5:
        explain = st.checkbox(
            "EXPLAIN ANALYZE (read-only)",
            value=False,
            help="Prepends EXPLAIN (ANALYZE, BUFFERS, VERBOSE) to each statement.",
        )

sql_txt = st.text_area(
    "SQL to execute inside this DB (multiple statements supported)",
    height=200,
    placeholder="Paste one or more SQL statements separated by semicolons…",
)

if st.button("Run SQL"):
    stmts = split_sql_statements(sql_txt or "")
    if not stmts:
        st.warning("No SQL statements detected.")
        st.stop()

    # Choose connection autocmmit mode according to single_txn
    with st.spinner(f"Executing {len(stmts)} statement(s)…"):
        if single_txn:
            # One transaction for ALL statements. Roll back and stop on first error.
            with get_conn(db, auto_commit=False) as conn:
                try:
                    with conn:  # starts a txn; commit on success, rollback on exception
                        for idx, stmt in enumerate(stmts, start=1):
                            pretty = f"Statement {idx}"
                            with st.container(border=True):
                                st.markdown(f"**{pretty}**")
                                st.code(stmt, language="sql")
                                try:
                                    kind, payload, elapsed = run_one_statement(
                                        conn,
                                        stmt,
                                        lock_timeout_ms,
                                        statement_timeout_ms,
                                        max_rows,
                                        explain,
                                        in_txn=True,
                                    )
                                except psycopg2.errors.LockNotAvailable as e:
                                    st.error(f"Command aborted: table is locked by another session. [{getattr(e, 'pgcode', '')}]")
                                    raise
                                except psycopg2.errors.QueryCanceled as e:
                                    st.error(f"Command timed out (statement or lock timeout). [{getattr(e, 'pgcode', '')}]")
                                    raise
                                except Exception as e:
                                    # Bubble up to trigger rollback of the whole transaction
                                    if hasattr(e, "pgcode") or hasattr(e, "pgerror"):
                                        st.error(f"{type(e).__name__} [{getattr(e,'pgcode','')}]: {getattr(e,'pgerror','') or e}")
                                    else:
                                        st.error(f"{type(e).__name__}: {e}")
                                    raise

                                if kind == "result":
                                    st.dataframe(payload, use_container_width=True)
                                    st.caption(f"Returned {len(payload)} row(s) • {elapsed*1000:.0f} ms")
                                else:
                                    st.success(f"{payload} • {elapsed*1000:.0f} ms")

                except Exception:
                    st.warning("Transaction rolled back due to the error above.")
        else:
            # Autocommit per statement; continue on errors.
            with get_conn(db, auto_commit=True) as conn:
                for idx, stmt in enumerate(stmts, start=1):
                    pretty = f"Statement {idx}"
                    with st.container(border=True):
                        st.markdown(f"**{pretty}**")
                        st.code(stmt, language="sql")
                        try:
                            kind, payload, elapsed = run_one_statement(
                                conn,
                                stmt,
                                lock_timeout_ms,
                                statement_timeout_ms,
                                max_rows,
                                explain,
                                in_txn=False,
                            )
                        except psycopg2.errors.LockNotAvailable as e:
                            st.error(f"Command aborted: table is locked by another session. [{getattr(e, 'pgcode', '')}]")
                            continue
                        except psycopg2.errors.QueryCanceled as e:
                            st.error(f"Command timed out (statement or lock timeout). [{getattr(e, 'pgcode', '')}]")
                            continue
                        except Exception as e:
                            if hasattr(e, "pgcode") or hasattr(e, "pgerror"):
                                st.error(f"{type(e).__name__} [{getattr(e,'pgcode','')}]: {getattr(e,'pgerror','') or e}")
                            else:
                                st.error(f"{type(e).__name__}: {e}")
                            continue

                        if kind == "result":
                            st.dataframe(payload, use_container_width=True)
                            st.caption(f"Returned {len(payload)} row(s) • {elapsed*1000:.0f} ms")
                        else:
                            st.success(f"{payload} • {elapsed*1000:.0f} ms")
