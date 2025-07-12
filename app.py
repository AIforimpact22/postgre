# app.py
import streamlit as st

# ───────── page config ─────────
st.set_page_config(page_title="PostgreSQL Admin Portal", layout="wide")

# ───────── PIN gate ─────────
CORRECT_PIN = st.secrets["auth"]["pin"]        # expected "1212"

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    st.title("🔒 Enter PIN")
    pin_input = st.text_input("PIN", type="password", on_change=lambda: None)

    if st.button("Unlock"):
        if pin_input == CORRECT_PIN:
            st.session_state.authenticated = True
            st.experimental_rerun()
        else:
            st.error("Incorrect PIN")
    st.stop()                                  # block rest of UI until authenticated

# ───────── main landing page ─────────
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
