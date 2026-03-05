import sqlite3
from ..logger import log_line


def get_invoice(conn: sqlite3.Connection, invoice_id: int):
    log_line("REPOSITORY", "INFO", f"get_invoice id={invoice_id}")

    return conn.execute(
        """
        SELECT id, timestamp, original_name, status, final_name
        FROM invoices
        WHERE id = ?
        """,
        (invoice_id,),
    ).fetchone()


def list_invoices(conn: sqlite3.Connection, status: str | None, limit: int):
    log_line("REPOSITORY", "INFO", f"list_invoices status={status} limit={limit}")

    sql = """
    SELECT id, timestamp, original_name, status, final_name
    FROM invoices
    """

    params = []

    if status:
        sql += " WHERE status = ?"
        params.append(status)

    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)

    return conn.execute(sql, params).fetchall()


def update_invoice_status(conn: sqlite3.Connection, invoice_id: int, new_status: str):
    log_line("REPOSITORY", "INFO", f"update_invoice_status id={invoice_id} -> {new_status}")

    cur = conn.execute(
        "UPDATE invoices SET status = ? WHERE id = ?",
        (new_status, invoice_id),
    )

    conn.commit()

    return cur.rowcount


def get_status_metrics(conn: sqlite3.Connection):
    log_line("REPOSITORY", "INFO", "get_status_metrics")

    rows = conn.execute(
        """
        SELECT status, COUNT(*) as count
        FROM invoices
        GROUP BY status
        """
    ).fetchall()

    return rows

def insert_status_history(conn: sqlite3.Connection, invoice_id: int, changed_at: str, old_status: str, new_status: str):
    log_line("REPOSITORY", "INFO", f"insert_status_history invoice_id={invoice_id} {old_status}->{new_status}")

    conn.execute(
        """
        INSERT INTO invoice_status_history (invoice_id, changed_at, old_status, new_status)
        VALUES (?, ?, ?, ?)
        """,
        (invoice_id, changed_at, old_status, new_status),
    )
    conn.commit()


def list_status_history(conn: sqlite3.Connection, invoice_id: int, limit: int):
    log_line("REPOSITORY", "INFO", f"list_status_history invoice_id={invoice_id} limit={limit}")

    return conn.execute(
        """
        SELECT id, invoice_id, changed_at, old_status, new_status
        FROM invoice_status_history
        WHERE invoice_id = ?
        ORDER BY id DESC
        LIMIT ?
        """,
        (invoice_id, limit),
    ).fetchall()