import os
from datetime import datetime
import pyodbc
from src.logger import log_line

SCHEMA = os.environ.get("PUB400_SCHEMA", "PATAKI221")

def get_conn():
    # Direkt driver string (nálad ez működik)
    user = os.environ.get("PUB400_UID", "PATAKI22")
    pwd = os.environ.get("PUB400_PWD", "ysd-k9-w!8tMbrjW")
    system = os.environ.get("PUB400_SYSTEM", "pub400.com")

    if not pwd:
        raise RuntimeError("PUB400_PWD environment variable is missing")

    return pyodbc.connect(
        "DRIVER={IBM i Access ODBC Driver};"
        f"SYSTEM={system};"
        f"UID={user};"
        f"PWD={pwd};"
        "NAM=1;"
        "DBQ=QGPL;",
        autocommit=True
    )

def insert_invoice(original_name: str, status: str, final_name: str, created_at: str | None = None) -> None:
    created_at = created_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sql = f"""
        INSERT INTO {SCHEMA}.INVOICE_PIPELINE
        (ORIGINAL_NAME, STATUS, FINAL_NAME, CREATED_AT)
        VALUES (?, ?, ?, ?)
    """

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql, (original_name, status, final_name, created_at))
        log_line("IBMI", "INFO", f"DB2 insert ok: {SCHEMA}.INVOICE_PIPELINE {original_name} {status}")
    finally:
        conn.close()