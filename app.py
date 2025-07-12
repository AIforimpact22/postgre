import streamlit as st
import psycopg2
import pandas as pd
import re

st.set_page_config(page_title="PostgreSQL Admin Portal", layout="wide")
pg = st.secrets["superuser"]

# ─────────────────── Helpers ───────────────────
def get_conn(dbname=None):
    return psycopg2.connect(
        dbname=dbname or pg["dbname"],
        user=pg["user"],
        password=pg["password"],
        host=pg["host"],
        port=pg["port"]
    )

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
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='public' AND table_type='BASE TABLE';"
        )
        return [row[0] for row in cur.fetchall()]

def get_columns(dbname, table):
    with get_conn(dbname) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=%s
            ORDER BY ordinal_position;
            """,
            (table,)
        )
        return cur.fetchall()

def insert_row(dbname, table, data):
    with get_conn(dbname) as conn:
        cur = conn.cursor()
        cols = ", ".join(data.keys())
        vals_placeholders = ", ".join(["%s"] * len(data))
        sql = f'INSERT INTO "{table}" ({cols}) VALUES ({vals_placeholders})'
        cur.execute(sql, list(data.values()))
        conn.commit()

# ───────────────── Sidebar nav ─────────────────
st.sidebar.title("Admin Navigation")
PAGES = [
    "Create Database",
    "Edit Database",
    "Browse Tables",         # ← NEW PAGE
    "Connection Info",
    "Delete",
    "Manual Data Entry"
]

if "active_page" not in st.session_state:
    st.session_state.active_page = PAGES[0]

for p in PAGES:
    if st.sidebar.button(p, key=p):
        st.session_state.active_page = p

st.sidebar.markdown("---")
st.sidebar.caption("Powered by Streamlit & PostgreSQL")

page = st.session_state.active_page

# ───────────────── Create DB ────────────────────
if page == "Create Database":
    st.title("Create Database")
    db_name = st.text_input(
        "Database name (letters, numbers, underscores only)",
        max_chars=32, help="Must start with a letter."
    )
    db_valid = bool(re.match(r"^[A-Za-z][A-Za-z0-9_]*$", db_name))
    sql_extra = st.text_area(
        "Optional SQL to run *inside* the new database (e.g. CREATE TABLE ...)",
        height=140
    )

    if st.button("Create Database and Run SQL"):
        if not db_valid:
            st.error("Invalid name. Use letters, numbers, underscores; start with a letter.")
        else:
            try:
                conn = get_conn()
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute(f'CREATE DATABASE "{db_name}";')
                conn.close()
                st.success(f"Database `{db_name}` created.")
                if sql_extra.strip():
                    with get_conn(db_name) as new_conn:
                        with new_conn.cursor() as cur:
                            cur.execute(sql_extra)
                            if cur.description:
                                rows = cur.fetchall()
                                cols = [d[0] for d in cur.description]
                                st.dataframe(pd.DataFrame(rows, columns=cols))
                            else:
                                new_conn.commit()
                    st.success("Optional SQL executed inside new DB.")
            except psycopg2.errors.DuplicateDatabase:
                st.warning(f"Database `{db_name}` already exists.")
            except Exception as e:
                st.error(f"Error: {e}")

    if st.button("List All Databases"):
        dbs = list_databases()
        st.dataframe(pd.DataFrame({"Database": dbs}))

# ───────────────── Edit DB ──────────────────────
elif page == "Edit Database":
    st.title("Edit Database")
    dbs = list_databases()
    db_select = st.selectbox("Choose a database:", dbs)

    if db_select:
        st.subheader(f"Schema of `{db_select}`")
        schema_rows = get_schema(db_select)
        if schema_rows:
            tbl_map = {}
            for t, c, d in schema_rows:
                tbl_map.setdefault(t, []).append(f"{c} ({d})")
            for t, cols in tbl_map.items():
                st.markdown(f"**{t}**")
                st.write(", ".join(cols))
        else:
            st.info("No tables yet.")

        st.subheader("SQL Editor")
        sql_cmd = st.text_area("SQL to run in this database:", height=140)
        if st.button("Run SQL", key="run_sql"):
            with st.spinner("Running…"):
                try:
                    with get_conn(db_select) as conn:
                        with conn.cursor() as cur:
                            cur.execute(sql_cmd)
                            if cur.description:
                                rows = cur.fetchall()
                                cols = [d[0] for d in cur.description]
                                st.dataframe(pd.DataFrame(rows, columns=cols))
                            else:
                                conn.commit()
                                st.success("Command executed.")
                except Exception as e:
                    st.error(e)

# ───────────────── Browse Tables ───────────────
elif page == "Browse Tables":
    st.title("Browse Tables")
    dbs = list_databases()
    db_select = st.selectbox("Select a database:", dbs, key="browse_db")

    if db_select:
        tables = get_tables(db_select)
        if tables:
            table_select = st.selectbox("Select a table:", tables, key="browse_table")
            if table_select:
                cols = [c[0] for c in get_columns(db_select, table_select)]
                limit = st.number_input(
                    "Rows to display", min_value=1, max_value=20000, value=50
                )
                with get_conn(db_select) as conn:
                    cur = conn.cursor()
                    cur.execute(
                        f'SELECT * FROM "{table_select}" LIMIT %s;',
                        (limit,)
                    )
                    rows = cur.fetchall()
                st.subheader(f"`{table_select}` preview")
                st.dataframe(pd.DataFrame(rows, columns=cols))
        else:
            st.info("No tables found in this database.")

# ───────────────── Connection Info ─────────────
elif page == "Connection Info":
    st.title("Connection Info")
    dbs = list_databases()
    db_select = st.selectbox("Choose DB:", dbs, key="conninfo")
    if db_select:
        st.subheader("`.streamlit/secrets.toml` snippet")
        toml_block = f"""
[{db_select}]
host = "{pg['host']}"
port = {pg['port']}
user = "{pg['user']}"
password = "{pg['password']}"
dbname = "{db_select}"
"""
        st.code(toml_block.strip(), language="toml")
        st.caption("Copy-paste this into your secrets file.")

# ───────────────── Delete DB ────────────────────
elif page == "Delete":
    st.title("Delete Database")
    protected = {"postgres", "template0", "template1"}
    deletable = [d for d in list_databases() if d not in protected]

    if deletable:
        db_select = st.selectbox("Database to delete:", deletable)
        confirm = st.checkbox(
            f"⚠️ Permanently delete `{db_select}`?", key="confirm_del"
        )
        if st.button("Delete Database"):
            if not confirm:
                st.warning("Please confirm first.")
            else:
                try:
                    conn = get_conn()
                    conn.autocommit = True
                    with conn.cursor() as cur:
                        cur.execute(f'DROP DATABASE "{db_select}";')
                    conn.close()
                    st.success(f"`{db_select}` deleted.")
                except psycopg2.errors.ObjectInUse:
                    st.error("Database is in use. Disconnect users then retry.")
                except Exception as e:
                    st.error(e)
        if st.button("Refresh list"):
            st.experimental_rerun()
    else:
        st.info("No user databases to delete.")

# ──────────────── Manual Data Entry ─────────────
elif page == "Manual Data Entry":
    st.title("Manual Data Entry")
    dbs = list_databases()
    db_select = st.selectbox("Choose a database:", dbs, key="entrydb")
    if db_select:
        tables = get_tables(db_select)
        table = st.selectbox("Choose a table:", tables, key="entrytable")
        if table:
            cols = get_columns(db_select, table)
            st.write(f"Columns in `{table}`: {[c[0] for c in cols]}")
            with st.form(key="entry_form"):
                data = {}
                for col_name, data_type in cols:
                    if data_type.lower() in ("integer", "bigint") and col_name.endswith("id"):
                        continue
                    data[col_name] = st.text_input(f"{col_name} ({data_type})", key=col_name)
                submit = st.form_submit_button("Insert Row")
                if submit:
                    # convert types
                    for col_name, data_type in cols:
                        val = data[col_name]
                        if val == "":
                            data[col_name] = None
                        elif data_type in ("integer", "bigint"):
                            data[col_name] = int(val)
                        elif data_type == "double precision":
                            data[col_name] = float(val)
                    cleaned = {k: v for k, v in data.items() if v is not None}
                    try:
                        insert_row(db_select, table, cleaned)
                        st.success(f"Inserted into `{table}`: {cleaned}")
                    except Exception as e:
                        st.error(f"Insert failed: {e}")
