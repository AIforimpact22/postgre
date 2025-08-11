# pages/3_Browse_Tables.py
import json
import math
import decimal
from typing import Any

import streamlit as st
import pandas as pd
import psycopg2
from psycopg2 import sql
from db_utils import list_databases, get_conn, list_schemata_tables, get_table_columns_fq


st.title("Browse Tables")

# ──────────────────────────────────────────────────────────────────────────────
# Helpers: make DataFrame Arrow-friendly for Streamlit
# ──────────────────────────────────────────────────────────────────────────────

TRUE_SET = {"true", "t", "1", "yes", "y"}
FALSE_SET = {"false", "f", "0", "no", "n"}

def _maybe_bool_series(s: pd.Series) -> pd.Series:
    """Try to coerce object/string series into boolean if it only contains boolean-ish values."""
    vals = s.dropna().astype(str).str.strip().str.lower().unique()
    if len(vals) == 0:
        return s  # nothing to do
    if set(vals).issubset(TRUE_SET.union(FALSE_SET)):
        return s.astype(str).str.strip().str.lower().map(
            lambda x: True if x in TRUE_SET else False if x in FALSE_SET else pd.NA
        ).astype("boolean")
    return s

def _maybe_datetime_series(s: pd.Series) -> pd.Series:
    """Try to coerce to datetime if it looks like datetime-like strings."""
    # Heuristic: presence of '-', '/', ':' often indicates date/time strings
    sample = s.dropna().astype(str).head(20)
    if sample.str.contains(r"[-/:]").mean() >= 0.5:
        dt = pd.to_datetime(s, errors="coerce", utc=False)
        # Only accept if we converted *most* rows, to avoid trashing free text columns
        if dt.notna().mean() >= 0.6:
            return dt
    return s

def _safe_json_dumps(x: Any) -> str:
    try:
        return json.dumps(x, ensure_ascii=False, default=str)
    except Exception:
        return str(x)

def _normalize_object_cell(x: Any) -> Any:
    """
    Normalize non-primitive Python objects into Arrow-safe scalars/strings.
    - dict/list/tuple/set -> JSON string
    - Decimal -> float (or str if NaN/Inf)
    - bytes/bytearray/memoryview -> hex preview with length
    """
    if x is None:
        return None
    if isinstance(x, (dict, list, tuple, set)):
        return _safe_json_dumps(x)
    if isinstance(x, (decimal.Decimal, )):
        try:
            f = float(x)
            # Keep as string if weird float
            if math.isnan(f) or math.isinf(f):
                return str(x)
            return f
        except Exception:
            return str(x)
    if isinstance(x, (bytes, bytearray, memoryview)):
        b = bytes(x)
        # Short hex preview to avoid huge cells; adjust if you like
        preview = b.hex()
        if len(preview) > 64:
            preview = preview[:64] + "…"
        return f"<{len(b)} bytes> 0x{preview}"
    return x

def to_arrow_friendly(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make a DataFrame safe for pyarrow conversion used by Streamlit:
      1) Normalize exotic Python objects in object columns
      2) Try numeric conversion (coerce)
      3) Try boolean conversion
      4) Try datetime conversion
      5) Finally ensure remaining object columns are strings
    """
    if df.empty:
        return df

    df = df.copy()

    # Step 1: normalize exotic cells in object columns
    obj_cols = [c for c in df.columns if df[c].dtype == "object"]
    for c in obj_cols:
        df[c] = df[c].map(_normalize_object_cell)

    # Step 2/3/4: attempt typed coercions
    for c in df.columns:
        s = df[c]
        if s.dtype == "object":
            # numeric
            num_try = pd.to_numeric(s, errors="coerce")
            # accept numeric if it converted most non-null values
            if num_try.notna().mean() >= 0.6:
                # choose best-fitting integer if possible
                if (num_try.dropna() % 1 == 0).all():
                    try:
                        df[c] = num_try.astype("Int64")  # nullable integer
                    except Exception:
                        df[c] = num_try.astype("float64")
                else:
                    df[c] = num_try.astype("float64")
                continue

            # boolean
            bool_try = _maybe_bool_series(s)
            if str(bool_try.dtype) == "BooleanDtype":
                df[c] = bool_try
                continue

            # datetime
            dt_try = _maybe_datetime_series(s)
            if pd.api.types.is_datetime64_any_dtype(dt_try):
                df[c] = dt_try
                continue

            # Finally, force to plain strings to avoid ArrowInvalid
            df[c] = s.astype(str)

    return df


# ──────────────────────────────────────────────────────────────────────────────
# UI: pick DB & table
# ──────────────────────────────────────────────────────────────────────────────

db = st.selectbox("Database", list_databases())
if not db:
    st.stop()

tables = list_schemata_tables(db)
if not tables:
    st.info("No tables in this DB.")
    st.stop()

tbl_choice = st.selectbox("Table", tables, help="Format: schema.table")
schema, table = tbl_choice.split(".")

# LIMIT + OFFSET (server-side pagination)
limit = st.number_input("Rows to display (0 = all)", 0, 1_000_000, 50, help="Use 0 cautiously on very large tables.")
offset = 0
if limit > 0:
    # Show offset only when limiting
    offset = st.number_input("Offset (for paging)", 0, 1_000_000_000, 0)

# Optional: simple ORDER BY primary key or first column?
# Keeping behavior identical to the original (no ORDER BY), which is fastest.

# ──────────────────────────────────────────────────────────────────────────────
# Query + display
# ──────────────────────────────────────────────────────────────────────────────

with get_conn(db, auto_commit=True) as conn, conn.cursor() as cur:
    # Fail fast on locks and long scans
    cur.execute("SET LOCAL lock_timeout = '1500ms';")
    cur.execute("SET LOCAL statement_timeout = '30000ms';")

    try:
        if limit > 0:
            if offset > 0:
                cur.execute(
                    sql.SQL("SELECT * FROM {} OFFSET %s LIMIT %s")
                       .format(sql.Identifier(schema, table)),
                    (offset, limit),
                )
            else:
                cur.execute(
                    sql.SQL("SELECT * FROM {} LIMIT %s")
                       .format(sql.Identifier(schema, table)),
                    (limit,),
                )
        else:
            cur.execute(
                sql.SQL("SELECT * FROM {}")
                   .format(sql.Identifier(schema, table))
            )

        rows = cur.fetchall()
        cols = get_table_columns_fq(db, schema, table)
        df_raw = pd.DataFrame(rows, columns=cols)

        # Arrow-safe normalization to prevent:
        # ArrowInvalid: Could not convert '0' with type str: tried to convert to int64
        df = to_arrow_friendly(df_raw)

        st.caption(f"Showing {len(df):,} row(s)" + (f" starting at offset {offset:,}" if limit > 0 and offset else ""))

        st.dataframe(df, use_container_width=True, hide_index=True)

        # Download as CSV (exactly what you're viewing)
        csv = df.to_csv(index=False)
        st.download_button(
            "Download CSV",
            data=csv,
            file_name=f"{schema}.{table}" + (f"__offset_{offset}" if offset else "") + (f"__limit_{limit}" if limit else "") + ".csv",
            mime="text/csv",
        )

    except psycopg2.errors.LockNotAvailable:
        st.error("Table is locked by another session. Try again in a moment.")
    except psycopg2.errors.QueryCanceled:
        st.error("Query timed out.")
    except psycopg2.Error as e:
        st.error(f"Database error: {e.pgerror or e}")
