# app.py — shows logo only on pre-login, half size top-center, starts at login
import os
import time
import logging
import streamlit as st
from PIL import Image  # for resizing logo

# ──────────────────────────────────────────────────────────────────────────────
#  Debug / instrumentation toggles
# ──────────────────────────────────────────────────────────────────────────────
DEBUG_SQL   = os.getenv("DEBUG_SQL", "1") == "1"
SHOW_SQL_UI = os.getenv("SHOW_SQL_UI", "0") == "1"

if DEBUG_SQL:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)s  %(message)s",
        datefmt="%H:%M:%S",
    )

# ──────────────────────────────────────────────────────────────────────────────
#  Monkey-patch mysql-connector so every cursor.execute() is timed
# ──────────────────────────────────────────────────────────────────────────────
def _patch_mysql_execute() -> None:
    if not DEBUG_SQL:
        return
    import mysql.connector
    if getattr(mysql.connector, "_timed_execute_patched", False):
        return
    real_exec = mysql.connector.cursor.MySQLCursor.execute
    timings_key = "_sql_timings"

    def timed_exec(self, operation, params=None, multi=False):
        t0 = time.perf_counter()
        result = real_exec(self, operation, params=params, multi=multi)
        dur_ms = (time.perf_counter() - t0) * 1000
        logging.info("[SQL] %7.1f ms  %s",
                     dur_ms,
                     (operation if isinstance(operation, str) else str(operation)).split()[0])
        if SHOW_SQL_UI:
            st.session_state.setdefault(timings_key, []).append(
                (operation.split()[0], dur_ms)
            )
        return result

    mysql.connector.cursor.MySQLCursor.execute = timed_exec
    mysql.connector._timed_execute_patched = True

_patch_mysql_execute()

# ──────────────────────────────────────────────────────────────────────────────
#  Regular imports (after patch to avoid circular deps)
# ──────────────────────────────────────────────────────────────────────────────
from theme           import apply_dark_theme
from database        import create_tables
from sidebar         import show_sidebar
from style           import show_footer
from importlib       import import_module
from github_progress import get_user_progress

# ──────────────────────────────────────────────────────────────────────────────
#  Helpers
# ──────────────────────────────────────────────────────────────────────────────
def safe_rerun():
    if hasattr(st, "experimental_rerun"):
        st.experimental_rerun()
    elif hasattr(st, "rerun"):
        st.rerun()

def enforce_week_gating(selected: str) -> bool:
    if selected.startswith("modules_week"):
        try:
            week = int(selected.replace("modules_week", ""))
        except ValueError:
            return True
        if week == 1:
            return True
        required = {2: 10, 3: 12, 4: 12, 5: 7}
        username = st.session_state.get("username", "default_user")
        user_prog = get_user_progress(username)
        return user_prog.get(f"week{week-1}", 0) >= required.get(week, 0)
    return True

def show_logo():
    """Display logo.png at 50% of its original width, centered."""
    logo_path = os.path.join(os.path.dirname(__file__), "logo.png")
    if os.path.isfile(logo_path):
        img = Image.open(logo_path)
        half_width = img.width // 2
        c1, c2, c3 = st.columns([1, 2, 1])
        with c2:
            st.image(img, width=half_width)

# ──────────────────────────────────────────────────────────────────────────────
#  Main Streamlit app
# ──────────────────────────────────────────────────────────────────────────────
def main() -> None:
    build_t0 = time.perf_counter()

    st.set_page_config(
        page_title="Code for Impact",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    apply_dark_theme()
    create_tables()

    # Default landing page is login
    st.session_state.setdefault("page", "login")

    page      = st.session_state["page"]
    logged_in = st.session_state.get("logged_in", False)

    # ───────────────────────────────────────────────────────────────────────────
    #  Pre-login flows: show logo + login UI
    # ───────────────────────────────────────────────────────────────────────────
    if not logged_in:
        show_logo()
        st.markdown("---")
        if page == "login":
            import login; login.show_login_create_account()
        elif page == "loginx":
            st.warning("Course 2 Login is not available yet.")
            if st.button("Go Back"):
                st.session_state["page"] = "login"
                safe_rerun()
        elif page == "course2_app":
            from second.appx import appx; appx.show()
        else:
            # any other page defaults to login
            st.session_state["page"] = "login"
            safe_rerun()

    # ───────────────────────────────────────────────────────────────────────────
    #  Post-login flows: no logo here
    # ───────────────────────────────────────────────────────────────────────────
    else:
        show_sidebar()
        if page == "logout":
            st.session_state["logged_in"] = False
            st.session_state["page"]      = "login"
            safe_rerun()
            return

        if page == "home":
            import home as _home; _home.show_home()
        else:
            if page.startswith("modules_week") and not enforce_week_gating(page):
                st.warning("You must complete the previous week before accessing this section.")
                st.stop()
            try:
                module = import_module(page)
                if hasattr(module, "show"):
                    module.show()
                else:
                    st.warning("The selected module does not have a 'show()' function.")
            except ImportError as e:
                st.warning(f"Unknown selection: {e}")

    # ───────────────────────────────────────────────────────────────────────────
    #  Footer & instrumentation
    # ───────────────────────────────────────────────────────────────────────────
    show_footer()
    st.sidebar.info(f"⏱ Page build: {(time.perf_counter() - build_t0)*1000:.0f} ms")
    if SHOW_SQL_UI and "_sql_timings" in st.session_state:
        with st.sidebar.expander("SQL timings"):
            for verb, dur in st.session_state["_sql_timings"]:
                st.write(f"{verb:<6} {dur:,.1f} ms")

if __name__ == "__main__":
    main()
