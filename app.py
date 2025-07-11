import streamlit as st
import psycopg2

st.title("PostgreSQL Admin Console (using secrets.toml)")

# --- Connect using superuser credentials from secrets ---
pg = st.secrets["superuser"]

@st.cache_resource(show_spinner=False)
def get_conn():
    # autocommit needed for CREATE DATABASE!
    conn = psycopg2.connect(
        dbname=pg["dbname"],
        user=pg["user"],
        password=pg["password"],
        host=pg["host"],
        port=pg["port"]
    )
    conn.autocommit = True
    return conn

try:
    conn = get_conn()
    st.success(f"Connected to {pg['host']}:{pg['port']} as {pg['user']} (db: {pg['dbname']})")
except Exception as e:
    st.error(f"Connection failed: {e}")
    st.stop()

# --- SQL Command Window ---
st.subheader("SQL Command Window")
sql = st.text_area(
    "Enter SQL (e.g., CREATE DATABASE ..., CREATE TABLE ...)", 
    height=200, 
    placeholder="Write your SQL here..."
)

if st.button("Execute SQL"):
    try:
        cur = conn.cursor()
        # If more than one statement is entered, split by ';'
        stmts = [stmt.strip() for stmt in sql.split(";") if stmt.strip()]
        results = []
        for stmt in stmts:
            cur.execute(stmt)
            if cur.description:  # SELECT or RETURNING
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
                results.append((rows, columns))
        if results:
            for i, (rows, columns) in enumerate(results):
                st.write(f"Result of statement #{i+1}:")
                st.dataframe(rows, columns=columns)
        else:
            st.success("SQL executed successfully!")
        cur.close()
    except Exception as e:
        st.error(str(e))

if st.button("List Databases"):
    try:
        cur = conn.cursor()
        cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        dbs = cur.fetchall()
        st.write("Databases:", [db[0] for db in dbs])
        cur.close()
    except Exception as e:
        st.error(str(e))
