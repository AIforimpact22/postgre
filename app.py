# app.py
import streamlit as st

# ─────────────────── Page config ───────────────────
st.set_page_config(
    page_title="PostgreSQL Admin Portal",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─────────────────── Determine expected PIN ───────────────────
DEFAULT_PIN = "1212"

if "auth" in st.secrets and "pin" in st.secrets["auth"]:
    EXPECTED_PIN = st.secrets["auth"]["pin"]
elif "pin" in st.secrets:
    EXPECTED_PIN = st.secrets["pin"]
else:
    EXPECTED_PIN = DEFAULT_PIN   # fall back silently

# ─────────────────── PIN gate ───────────────────
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    st.title("🔒 Locked")
    pin_entry = st.text_input("Enter PIN", type="password")
    if st.button("Unlock") and pin_entry == EXPECTED_PIN:
        st.session_state.authenticated = True
    st.stop()  # block everything below until authenticated

# ─────────────────── Main landing page ───────────────────
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

All features share the same super-user credentials stored in **`.streamlit/secrets.toml`** (`[superuser]` block).
"""
)
st.info("Pick a page from the sidebar ⬅️ to get started.")
