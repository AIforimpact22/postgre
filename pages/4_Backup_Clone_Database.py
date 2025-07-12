# pages/4_Backup_Clone_Database.py
import streamlit as st
import datetime as dt
import psycopg2
from db_utils import list_databases, get_conn, valid_db

st.title("Backup / Clone Database")

src = st.selectbox("Database to clone", list_databases())
if not src:
    st.stop()

default_name = f"{src}_backup_{dt.datetime.now():%Y%m%d_%H%M}"
dest = st.text_input("Name for clone", default_name)

if st.button("Clone database"):
    if not valid_db(dest or ""):
        st.error("Invalid clone name.")
    elif dest == src:
        st.error("Clone name must differ.")
    else:
        try:
            conn = get_conn()
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f'CREATE DATABASE "{dest}" WITH TEMPLATE "{src}";')
            conn.close()
            st.success(f"Cloned **{src}** ➡️ **{dest}**")
        except psycopg2.errors.DuplicateDatabase:
            st.warning("Target DB already exists.")
        except psycopg2.errors.ObjectInUse:
            st.error("Source DB is in use—disconnect active sessions.")
        except Exception as e:
            st.error(e)
