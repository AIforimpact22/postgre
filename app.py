import streamlit as st
import psycopg2

st.title("PostgreSQL Admin Console (Neon Style)")

# 1. Connect with superuser credentials from secrets.toml
pg = st.secrets["superuser"]

@st.cache_resource(show_spinner=False)
def get_conn():
    return psycopg2.connect(
        dbname=pg["dbname"],
        user=pg["user"],
        password=pg["password"],
        host=pg["host"],
        port=pg["port"]
    )

try:
    conn = get_conn()
    st.success(f"Connected to {pg['host']} as {pg['user']} (db: {pg['dbname']})")
except Exception as e:
    st.error(f"Connection failed: {e}")
    st.stop()

# 2. SQL Command Window (like Neon)
st.subheader("SQL Command Window")
sql = st.text_area(
    "Enter SQL (e.g., CREATE DATABASE yourdb; CREATE TABLE ...; SELECT ...):",
    height=150,
    key="sql_command"
)

if st.button("Run SQL Command"):
    with st.spinner("Running SQL..."):
        try:
            cur = conn.cursor()
            cur.execute(sql)
            # For SELECT queries, display results
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

# 3. List all databases (like Neon DB browser)
if st.button("List All Databases"):
    with st.spinner("Fetching databases..."):
        try:
            cur = conn.cursor()
            cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
            dbs = [db[0] for db in cur.fetchall()]
            st.write("Available databases:")
            st.table(dbs)
            cur.close()
        except Exception as e:
            st.error(f"Error: {e}")

st.markdown("---")
st.info("To create a new database, use: `CREATE DATABASE yourdbname;` and click **Run SQL Command**. "
        "To create tables, use `CREATE TABLE ...` after connecting to the target database. "
        "Use **List All Databases** to refresh and see your new database instantly.")
