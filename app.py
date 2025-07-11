import streamlit as st
import psycopg2
import re

st.set_page_config(page_title="PostgreSQL Admin Portal", layout="wide")

pg = st.secrets["superuser"]

def get_conn(dbname=None):
    # Use the requested db or fall back to superuser/admin db
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
        cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname;")
        dbs = [db[0] for db in cur.fetchall()]
        cur.close()
        return dbs

def get_schema(dbname):
    with get_conn(dbname) as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position;
        """)
        rows = cur.fetchall()
        cur.close()
        return rows

# ───── Sidebar Navigation ─────
page = st.sidebar.radio("Choose a page:", ["Create Database", "Edit Database"])

# ───── Create Database Page ─────
if page == "Create Database":
    st.title("PostgreSQL Admin Portal - Create Database")
    db_name = st.text_input(
        "Database name (letters, numbers, underscores only)",
        max_chars=32,
        help="Only letters, numbers, and underscores (_). Must start with a letter."
    )
    db_valid = bool(re.match(r'^[A-Za-z][A-Za-z0-9_]*$', db_name))

    if st.button("Create Database"):
        if not db_valid:
            st.error("Invalid database name. Use only letters, numbers, and underscores, starting with a letter.")
        else:
            try:
                with get_conn() as conn:
                    cur = conn.cursor()
                    cur.execute(f'CREATE DATABASE "{db_name}";')
                    conn.commit()
                    st.success(f'Database `{db_name}` created successfully!')
                    cur.close()
            except psycopg2.errors.DuplicateDatabase:
                st.warning(f"Database `{db_name}` already exists.")
            except Exception as e:
                st.error(f"Error: {e}")

    if st.button("List All Databases"):
        dbs = list_databases()
        st.write("Available databases:")
        st.table(dbs)

# ───── Edit Database Page ─────
elif page == "Edit Database":
    st.title("PostgreSQL Admin Portal - Edit Database")

    dbs = list_databases()
    db_select = st.selectbox("Choose a database to edit:", dbs)

    if db_select:
        st.subheader(f"Schema for `{db_select}`")
        schema = get_schema(db_select)
        if schema:
            schema_dict = {}
            for tbl, col, typ in schema:
                schema_dict.setdefault(tbl, []).append(f"{col} ({typ})")
            for table, cols in schema_dict.items():
                st.markdown(f"**{table}**")
                st.write(", ".join(cols))
        else:
            st.info("No tables in this database yet.")

        st.subheader("SQL Editor")
        sql = st.text_area(
            f"Run SQL in `{db_select}` (e.g., CREATE TABLE, SELECT, etc.):",
            height=120,
            key="sql_editor"
        )
        if st.button("Run SQL", key="run_sql"):
            with st.spinner("Running SQL..."):
                try:
                    with get_conn(db_select) as conn:
                        cur = conn.cursor()
                        cur.execute(sql)
                        if cur.description:
                            rows = cur.fetchall()
                            columns = [desc[0] for desc in cur.description]
                            st.dataframe(rows, columns=columns)
                            st.success("Query executed and results shown above.")
                        else:
                            conn.commit()
                            st.success("SQL command executed successfully!")
                        cur.close()
                except Exception as e:
                    st.error(f"Error: {e}")
