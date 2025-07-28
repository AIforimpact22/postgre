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

def highlight_row(row):
    if row["state"] == "idle in transaction":
        return ['background-color: #f59e42; color: #fff'] * len(row)
    if str(row["wait_event_type"]).strip().lower() not in ["", "none", "null"] and pd.notnull(row["wait_event_type"]):
        return ['background-color: #f43f5e; color: #fff'] * len(row)
    return [''] * len(row)

st.dataframe(
    df.style.apply(highlight_row, axis=1),
    use_container_width=True,
    hide_index=True,
    height=min(800, max(400, len(df)*38)),
)

st.write("### Terminate Connections")
for idx, row in df.iterrows():
    pid = row["pid"]
    cols = st.columns([6, 1])
    with cols[0]:
        st.write(f"PID: `{pid}` | User: `{row['user']}` | DB: `{row['database']}` | State: **{row['state']}**")
        st.code(str(row["query"]), language="sql")
    with cols[1]:
        if st.button(f"Terminate", key=f"kill{pid}"):
            terminate_connection(pid)
            st.warning(f"Terminated connection PID {pid}")
            st.rerun()

st.markdown(f"""
**Total connections:** `{len(df)}`  
**Unique users:** `{df['user'].nunique()}`  
**Unique databases:** `{df['database'].nunique()}`
""")

if st.button("Refresh connections list"):
    st.rerun()
