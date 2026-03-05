import os
import pyodbc
from src.logger import log_line

# DSN alapú csatlakozás (Windows ODBC Data Sources)
# A DSN-t majd később hozzuk létre: pl. "PUB400_DB2"
DSN_NAME = os.environ.get("PUB400_DSN", "PUB400_DB2")


def get_conn():
    log_line("IBMI", "INFO", f"DB2 connect via DSN={DSN_NAME}")
    # DSN beállítás: ODBC Data Source Administrator
    return pyodbc.connect(f"DSN={DSN_NAME};", autocommit=True)


def ping() -> dict:
    # IBM i DB2 “dummy” query. Név változhat, de ez a klasszikus minta.
    sql = "SELECT CURRENT DATE AS today FROM SYSIBM.SYSDUMMY1"

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        return {"status": "ok", "today": str(row[0])}
    finally:
        conn.close()