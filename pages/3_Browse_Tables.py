# pages/3_Browse_Tables.py
import json
import math
import decimal
from typing import Any, List, Optional, Tuple

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
# Ordering helpers (to show "last inserted" first when possible)
# ──────────────────────────────────────────────────────────────────────────────

def get_primary_key_columns(conn, schema: str, table: str) -> List[str]:
    """
    Return primary key column names in order, or [] if none.
    """
    q = """
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a
          ON a.attrelid = i.indrelid
         AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = %s::regclass
          AND i.indisprimary
        ORDER BY array_position(i.indkey, a.attnum);
    """
    fq = f"{schema}.{table}"
    with conn.cursor() as c:
        c.execute(q, (fq,))
        return [r[0] for r in c.fetchall()]

def get_columns_with_types(conn, schema: str, table: str) -> List[Tuple[str, str]]:
    """
    Return [(column_name, udt_name)] for the table.
    """
    q = """
        SELECT a.attname, t.typname
        FROM pg_attribute a
        JOIN pg_type t ON t.oid = a.atttypid
        WHERE a.attrelid = %s::regclass
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY a.attnum;
    """
    fq = f"{schema}.{table}"
    with conn.cursor() as c:
        c.execute(q, (fq,))
        return [(r[0], r[1]) for r in c.fetchall()]

COMMON_TS_NAMES = {"created_at", "updated_at", "inserted_at", "createdon", "updatedon", "timestamp", "ts", "created", "updated"}
COMMON_ID_NAMES = {"id"}

def pick_ordering_columns(conn, schema: str, table: str) -> Tuple[List[str], str]:
    """
    Heuristic to pick an ORDER BY that likely returns newest rows first.
    Returns (columns, strategy) where columns is a list of column names to ORDER BY DESC.
    Strategy is a short label for UI.
    """
    # 1) Primary key (DESC)
    pk_cols = get_primary_key_columns(conn, schema, table)
    if pk_cols:
        return pk_cols, "primary key"

    # 2) Timestamp-ish columns (DESC)
    cols_types = get_columns_with_types(conn, schema, table)
    # Prefer timestamp/timestamptz/date columns by common names
    ts_candidates = []
    for name, typ in cols_types:
        typ_l = (typ or "").lower()
        if typ_l in {"timestamptz", "timestamp", "timestampz", "timestamp without time zone", "timestamp with time zone", "date"}:
            ts_candidates.append(name)
    # sort by whether name looks common, then keep first
    if ts_candidates:
        ts_candidates.sort(key=lambda n: (0 if n.lower() in COMMON_TS_NAMES else 1, n))
        return [ts_candidates[0]], "timestamp"

    # 3) Numeric id-like columns (DESC)
    id_candidates = []
    for name, typ in cols_types:
        typ_l = (typ or "").lower()
        if name.lower() in COMMON_ID_NAMES or name.lower().endswith("_id"):
            if typ_l in {"int2", "int4", "int8", "serial", "bigserial", "numeric"}:
                id_candidates.append(name)
    if id_candidates:
        id_candidates.sort(key=lambda n: (0 if n.lower() in COMMON_ID_NAMES else 1, n))
        return [id_candidates[0]], "id"

    # 4) Fallback: physical order (ctid) DESC — heuristic only
    return ["ctid"], "ctid"


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

# Order preference
newest_first = st.checkbox("Newest first (DESC)", value=True, help="When enabled, attempts to show the most recently inserted rows first (based on PK/timestamp/id, else ctid).")

# ──────────────────────────────────────────────────────────────────────────────
# Query + display
# ──────────────────────────────────────────────────────────────────────────────

with get_conn(db, auto_commit=True) as conn, conn.cursor() as cur:
    # Fail fast on locks and long scans
    cur.execute("SET LOCAL lock_timeout = '1500ms';")
    cur.execute("SET LOCAL statement_timeout = '30000ms';")

    # Decide ORDER BY
    order_cols: Optional[List[str]] = None
    order_strategy = None
    if newest_first:
        try:
            order_cols, order_strategy = pick_ordering_columns(conn, schema, table)
        except Exception:
            # If catalog lookup fails for any reason, fall back to no explicit ordering
            order_cols, order_strategy = None, None

    try:
        # Build SELECT
        base = sql.SQL("SELECT * FROM {}").format(sql.Identifier(schema, table))

        # ORDER BY
        if order_cols:
            order_exprs = [
                (sql.Identifier(c) if c != "ctid" else sql.SQL("ctid"))  # ctid is a system column, not an identifier
                for c in order_cols
            ]
            # DESC for each chosen column
            order_by = sql.SQL(", ").join([sql.SQL("{} DESC").format(e) for e in order_exprs])
            base = base + sql.SQL(" ORDER BY ") + order_by

        # OFFSET/LIMIT
        if limit > 0 and offset > 0:
            q = base + sql.SQL(" OFFSET %s LIMIT %s")
            params = (offset, limit)
        elif limit > 0:
            q = base + sql.SQL(" LIMIT %s")
            params = (limit,)
        else:
            q = base
            params = ()

        cur.execute(q, params)

        rows = cur.fetchall()
        cols = get_table_columns_fq(db, schema, table)
        df_raw = pd.DataFrame(rows, columns=cols)

        # Arrow-safe normalization to prevent:
        # ArrowInvalid: Could not convert '0' with type str: tried to convert to int64
        df = to_arrow_friendly(df_raw)

        caption_bits = [f"Showing {len(df):,} row(s)"]
        if limit > 0 and offset:
            caption_bits.append(f"starting at offset {offset:,}")
        if newest_first and order_strategy:
            caption_bits.append(f"ordered by {order_strategy} DESC")
        elif newest_first:
            caption_bits.append("ordered DESC")
        st.caption(" • ".join(caption_bits))

        st.dataframe(df, use_container_width=True, hide_index=True)

        # Download as CSV (exactly what you're viewing)
        csv = df.to_csv(index=False)
        st.download_button(
            "Download CSV",
            data=csv,
            file_name=f"{schema}.{table}" + (f"__offset_{offset}" if offset else "") + (f"__limit_{limit}" if limit else "") + ( "__desc" if newest_first else "" ) + ".csv",
            mime="text/csv",
        )

    except psycopg2.errors.LockNotAvailable:
        st.error("Table is locked by another session. Try again in a moment.")
    except psycopg2.errors.QueryCanceled:
        st.error("Query timed out.")
    except psycopg2.Error as e:
        st.error(f"Database error: {e.pgerror or e}")
