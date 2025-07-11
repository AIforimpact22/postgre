import streamlit as st
import psycopg2

st.title("PostgreSQL Admin Console")

# --- Credentials for superuser (usually 'postgres') ---
st.sidebar.header("Server Connection")
host = st.sidebar.text_input("Host", value="188.36.44.146")
port = st.sidebar.number_input("Port", value=5432, step=1)
user = st.sidebar.text_input("User", value="postgres")
password = st.sidebar.text_input("Password", value="", type="password")
default_db = st.sidebar.text_input("Connect to database", value="postgres")

# --- Connect Button ---
if st.sidebar.button("Connect"):
    try:
        conn = psycopg2.connect(
            dbname=default_db,
            user=user,
            password=password,
            host=host,
            port=port
        )
        st.session_state["conn"] = conn
        st.success("Connected!")
    except Exception as e:
        st.error(f"Connection failed: {e}")

# --- SQL Execution Window ---
if "conn" in st.session_state:
    st.subheader("SQL Command Window")
    sql = st.text_area("Enter your SQL (e.g., CREATE DATABASE, CREATE TABLE ...)", height=200)
    if st.button("Execute SQL"):
        conn = st.session_state["conn"]
        try:
            cur = conn.cursor()
            cur.execute(sql)
            # Fetch results if any (for SELECT queries)
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

    # Optional: List all databases
    if st.button("List Databases"):
        conn = st.session_state["conn"]
        try:
            cur = conn.cursor()
            cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
            dbs = cur.fetchall()
            st.write("Databases:", [db[0] for db in dbs])
            cur.close()
        except Exception as e:
            st.error(str(e))
