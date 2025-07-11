import streamlit as st
import psycopg2

st.title("PostgreSQL Admin Console (using secrets.toml)")

# --- Connect automatically with superuser credentials from secrets ---
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
    st.success(f"Connected to {pg['host']}:{pg['port']} as {pg['user']} (db: {pg['dbname']})")
except Exception as e:
    st.error(f"Connection failed: {e}")
    st.stop()

# --- SQL Command Window ---
st.subheader("SQL Command Window")
sql = st.text_area("Enter SQL (e.g., CREATE DATABASE ..., CREATE TABLE ...)", height=200)

if st.button("Execute SQL"):
    try:
        cur = conn.cursor()
        cur.execute(sql)
        # Fetch results if any (e.g. SELECT)
        if cur.description:
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            st.dataframe(rows, columns=columns)
        else:
            conn.commit()
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
