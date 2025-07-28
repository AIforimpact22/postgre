# connections.py
import streamlit as st
import pandas as pd
import psycopg2
from db_utils import get_conn

st.title("🧑‍💻 Active PostgreSQL Connections")

# Show only for admins
if not st.session_state.get("authenticated", False):
    st.warning("You must unlock the app to use this page.")
    st.stop()

# Query connections
try:
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
        df = pd.DataFrame(rows, columns=cols)
except Exception as e:
    st.error(f"Could not query connections: {e}")
    st.stop()

if df.empty:
    st.info("No active connections.")
else:
    st.caption("Showing all active connections (across all databases you have access to).")
    st.write("**Legend:**")
    st.markdown("""
- <span style="color:#f59e42;">**idle in transaction**</span> or <span style="color:#f43f5e;">**waiting**</span>: possible connection/locking issues.
""", unsafe_allow_html=True)

    # Color rows
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

    st.markdown(f"""
**Total connections:** `{len(df)}`  
**Unique users:** `{df['user'].nunique()}`  
**Unique databases:** `{df['database'].nunique()}`
""")

    if st.button("Refresh connections list"):
        st.experimental_rerun()
