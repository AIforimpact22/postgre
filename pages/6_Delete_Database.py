# pages/6_Delete_Database.py
import streamlit as st
import psycopg2
from db_utils import list_databases, get_conn

st.title("Delete Database ⚠️")

protected = {"postgres", "template0", "template1"}
choices = [d for d in list_databases() if d not in protected]

if not choices:
    st.info("No user databases to delete.")
    st.stop()

db = st.selectbox("Select DB to drop", choices)
confirm = st.checkbox("Yes, really delete it.")

if st.button("Drop database"):
    if not confirm:
        st.warning("Please tick the confirmation box.")
    else:
        try:
            conn = get_conn()
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f'DROP DATABASE "{db}";')
            conn.close()
            st.success(f"Database **{db}** deleted.")
        except psycopg2.errors.ObjectInUse:
            st.error("Database is in use; close connections first.")
        except Exception as e:
            st.error(e)
