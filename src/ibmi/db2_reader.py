import os
import pyodbc
from src.logger import log_line

def _conn_str() -> str:
    # DSN-t használunk, mert már felvetted Windows ODBC-ben (PUB400)
    uid = os.environ.get("PUB400_UID", "PATAKI22")
    pwd = os.environ.get("PUB400_PWD", "ysd-k9-w!8tMbrjW")
    return f"DSN=PUB400;UID={uid};PWD={pwd}"

def get_schema() -> str:
    return os.environ.get("PUB400_SCHEMA", "PATAKI221")

def get_connection():
    # autocommit readhez nem kötelező, de oké
    return pyodbc.connect(_conn_str(), autocommit=True)

def list_invoices(limit: int = 50):
    schema = get_schema()
    sql = f"""
        SELECT ID, ORIGINAL_NAME, STATUS, FINAL_NAME, CREATED_AT
        FROM {schema}.INVOICE_PIPELINE
        ORDER BY ID DESC
        FETCH FIRST ? ROWS ONLY
    """

    conn = get_connection()
    try:
        cur = conn.cursor()
        log_line("IBMI", "INFO", f"DB2 list_invoices limit={limit}")
        cur.execute(sql, (limit,))
        rows = cur.fetchall()

        # pyodbc row -> dict
        out = []
        for r in rows:
            out.append({
                "id": int(r.ID),
                "original_name": str(r.ORIGINAL_NAME),
                "status": str(r.STATUS),
                "final_name": str(r.FINAL_NAME),
                "created_at": str(r.CREATED_AT),
            })
        return out
    finally:
        conn.close()