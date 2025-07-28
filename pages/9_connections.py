import streamlit as st
import pandas as pd
from db_utils import get_conn

st.title("🧑‍💻 Active PostgreSQL Connections (with Terminate)")

def get_activity():
    with get_conn(auto_commit=True) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT
                pid,
                usename AS user,
                datname AS database,
                client_addr,
                application_name,
                backend_start,
                state,
                wait_event_type,
                query_start,
                state_change,
                query
            FROM pg_stat_activity
            WHERE datname IS NOT NULL
            ORDER BY datname, state DESC, backend_start DESC
        """)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(rows, columns=cols)

def terminate_connection(pid):
    with get_conn(auto_commit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_terminate_backend(%s)", (pid,))

# Get and show connections
df = get_activity()
if df.empty:
    st.info("No active connections.")
    st.stop()

st.caption("You can terminate (kill) background or stuck connections directly from here.")

# Choose connections to kill
st.write("### Active Connections")
for idx, row in df.iterrows():
    col1, col2, col3 = st.columns([3, 5, 1])
    with col1:
        st.write(f"**PID:** `{row['pid']}` | **User:** `{row['user']}` | **DB:** `{row['database']}`")
        st.write(f"Client: `{row['client_addr']}` | State: **{row['state']}**")
        if str(row["wait_event_type"]).strip().lower() not in ["", "none", "null"] and pd.notnull(row["wait_event_type"]):
            st.warning(f"Waiting event: {row['wait_event_type']}")
        st.code(str(row["query"]), language="sql")
    with col2:
        st.write(f"Started: {row['backend_start']}")
        st.write(f"Query start: {row['query_start']}")
        st.write(f"Last state change: {row['state_change']}")
    with col3:
        if row['state'] == "idle in transaction" or st.button(f"Terminate PID {row['pid']}", key=f"kill{row['pid']}"):
            terminate_connection(row["pid"])
            st.warning(f"Terminated connection PID {row['pid']}")
            st.experimental_rerun()
    st.markdown("---")

st.markdown(f"""
**Total connections:** `{len(df)}`  
**Unique users:** `{df['user'].nunique()}`  
**Unique databases:** `{df['database'].nunique()}`
""")

if st.button("Refresh connections list"):
    st.experimental_rerun()
