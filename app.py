# app.py
import streamlit as st

st.set_page_config(page_title="PostgreSQL Admin Portal", layout="wide")

st.title("PostgreSQL Admin Portal 🐘")
st.markdown(
    """
Welcome! Use the sidebar to:

1. **Create** and initialise databases  
2. **Edit** schemas & run ad‑hoc SQL  
3. **Browse** table contents  
4. **Clone / back‑up** a database in‑place  
5. Grab **connection snippets**  
6. **Delete** obsolete databases  
7. Perform **manual data entry**

All features share the same super‑user credentials stored securely in **`.streamlit/secrets.toml`** (`[superuser]` block).
"""
)

st.info("Pick a page from the sidebar ⬅️ to get started.")
