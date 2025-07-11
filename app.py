import streamlit as st
import psycopg2

st.title("PostgreSQL Admin Console (with Diagnostics)")

# --- Connect using superuser credentials from secrets ---
pg = st.secrets["superuser"]

@st.cache_resource(show_spinner=False)
def get_conn():
    conn = psycopg2.connect(
        dbname=pg["dbname"],
        user=pg["user"],
        password=pg["password"],
        host=pg["host"],
        port=pg["port"]
    )
    conn.autocommit = True  # Required for CREATE DATABASE
    return conn

def show_user_permissions(conn):
    try:
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_USER;")
        current_user = cur.fetchone()[0]

        cur.execute("""
            SELECT rolname, rolsuper, rolcreatedb, rolcreaterole, rolcanlogin
            FROM pg_roles WHERE rolname = %s;
        """, (current_user,))
        perms = cur.fetchone()
        st.info(
            f"User: **{perms[0]}**\n\n"
            f"- Superuser: `{perms[1]}`\n"
            f"- Can CREATE DATABASE: `{perms[2]}`\n"
            f"- Can CREATE ROLE: `{perms[3]}`\n"
            f"- Can Login: `{perms[4]}`"
        )
        cur.execute("""
            SELECT r.rolname
            FROM pg_auth_members m
            JOIN pg_roles r ON m.roleid = r.oid
            WHERE member = (SELECT oid FROM pg_roles WHERE rolname = %s)
        """, (current_user,))
        groups = [row[0] for row in cur.fetchall()]
        st.info(f"User `{current_user}` belongs to groups: {groups}")
        cur.close()
    except Exception as e:
        st.warning(f"Could not check user permissions: {e}")

try:
    conn = get_conn()
    st.success(f"Connected to {pg['host']}:{pg['port']} as {pg['user']} (db: {pg['dbname']})")
    show_user_permissions(conn)
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
        stmts = [stmt.strip() for stmt in sql.split(";") if stmt.strip()]
        results = []
        for stmt in stmts:
            cur.execute(stmt)
            if cur.description:
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
        st.error(f"Error: {e}")
        st.info("Check your user permissions above for clues.")
        show_user_permissions(conn)

if st.button("List Databases"):
    try:
        cur = conn.cursor()
        cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        dbs = cur.fetchall()
        st.write("Databases:", [db[0] for db in dbs])
        cur.close()
    except Exception as e:
        st.error(str(e))
