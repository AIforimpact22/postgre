# pages/5_Connection_Info.py
import streamlit as st
from db_utils import list_databases, pg  # pg pulled from st.secrets in db_utils

st.title("Connection Info Snippet")

db = st.selectbox("Database", list_databases())
if db:
    st.subheader("Copy this into `.streamlit/secrets.toml`")
    st.code(
        f"""
[{db}]
host = "{pg['host']}"
port = {pg['port']}
user = "{pg['user']}"
password = "{pg['password']}"
dbname = "{db}"
""",
        language="toml",
    )
