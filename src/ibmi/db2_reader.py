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

def list_invoices(limit: int = 50, status: str | None = None):
    schema = get_schema()

    where = ""
    params = []
    if status:
        where = "WHERE STATUS = ?"
        params.append(status)

    sql = f"""
        SELECT ID, ORIGINAL_NAME, STATUS, FINAL_NAME, CREATED_AT
        FROM {schema}.INVOICE_PIPELINE
        {where}
        ORDER BY ID DESC
        FETCH FIRST ? ROWS ONLY
    """
    params.append(limit)

    conn = get_connection()
    try:
        cur = conn.cursor()
        log_line("IBMI", "INFO", f"DB2 list_invoices status={status} limit={limit}")
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()

        out = []
        for r in rows:
            out.append({
                "id": int(r.ID),
                "original_name": str(r.ORIGINAL_NAME),
                "status": str(r.STATUS).strip(),
                "final_name": str(r.FINAL_NAME),
                "created_at": str(r.CREATED_AT),
            })
        return out
    finally:
        conn.close()

def get_invoice(invoice_id: int):
    schema = get_schema()
    sql = f"""
        SELECT ID, ORIGINAL_NAME, STATUS, FINAL_NAME, CREATED_AT
        FROM {schema}.INVOICE_PIPELINE
        WHERE ID = ?
        FETCH FIRST 1 ROWS ONLY
    """

    conn = get_connection()
    try:
        cur = conn.cursor()
        log_line("IBMI", "INFO", f"DB2 get_invoice id={invoice_id}")
        cur.execute(sql, (invoice_id,))
        r = cur.fetchone()
        if not r:
            return None
        return {
            "id": int(r.ID),
            "original_name": str(r.ORIGINAL_NAME),
            "status": str(r.STATUS).strip(),
            "final_name": str(r.FINAL_NAME),
            "created_at": str(r.CREATED_AT),
        }
    finally:
        conn.close()


def update_invoice_status(invoice_id: int, new_status: str) -> int:
    schema = get_schema()
    sql = f"""
        UPDATE {schema}.INVOICE_PIPELINE
        SET STATUS = ?
        WHERE ID = ?
    """

    conn = get_connection()
    try:
        cur = conn.cursor()
        log_line("IBMI", "INFO", f"DB2 update_invoice_status id={invoice_id} -> {new_status}")
        cur.execute(sql, (new_status, invoice_id))
        return cur.rowcount
    finally:
        conn.close()


def get_status_metrics():
    schema = get_schema()
    sql = f"""
        SELECT STATUS, COUNT(*) AS CNT
        FROM {schema}.INVOICE_PIPELINE
        GROUP BY STATUS
    """

    conn = get_connection()
    try:
        cur = conn.cursor()
        log_line("IBMI", "INFO", "DB2 get_status_metrics")
        rows = cur.execute(sql).fetchall()
        return {str(r.STATUS).strip(): int(r.CNT) for r in rows}
    finally:
        conn.close()
