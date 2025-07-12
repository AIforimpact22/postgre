# app.py
import streamlit as st

# ───────────────────────────────
# Page-wide settings
# ───────────────────────────────
st.set_page_config(
    page_title="PostgreSQL Admin Portal",
    layout="wide",
    initial_sidebar_state="collapsed",   # keep sidebar hidden until unlocked
)

# ───────────────────────────────
# PIN-gate logic
# ───────────────────────────────
PIN = st.secrets["pin"]          # expect "1212" in .streamlit/secrets.toml

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    st.title("🔒 Locked")
    st.write("Enter PIN to access the admin portal.")
    pin_entry = st.text_input("PIN", type="password")

    if st.button("Unlock"):
        if pin_entry == PIN:
            st.session_state.authenticated = True
            st.experimental_rerun()
        else:
            st.error("Incorrect PIN")
    st.stop()                   # ⇦ absolutely nothing below this line executes

# ───────────────────────────────
# Main landing content (shown only after PIN)
# ───────────────────────────────
st.title("PostgreSQL Admin Portal 🐘")
st.markdown(
    """
Welcome! Use the sidebar to:

1. **Create** and initialise databases  
2. **Edit** schemas & run ad-hoc SQL  
3. **Browse** table contents  
4. **Clone / back-up** a database in-place  
5. Grab **connection snippets**  
6. **Delete** obsolete databases  
7. Perform **manual data entry**  
8. **Bulk-upload** CSV files

All features share the same super-user credentials stored securely in **`.streamlit/secrets.toml`** (`[superuser]` block).
"""
)

st.info("Pick a page from the sidebar ⬅️ to get started.")
