from src.logger import log_line
from src.ibmi.db2_reader import get_connection, get_status_metrics, list_invoices, get_schema

def db2_ping():
    """
    Small safe IBM i DB2 action.
    Verifies that the Python app can reach DB2 on IBM i.
    """
    sql = "SELECT CURRENT DATE AS CUR_DATE, CURRENT TIME AS CUR_TIME FROM SYSIBM.SYSDUMMY1"

    conn = get_connection()
    try:
        cur = conn.cursor()
        log_line("IBMI", "INFO", "ACTION db2_ping started")
        row = cur.execute(sql).fetchone()

        result = {
            "ok": True,
            "action": "db2_ping",
            "current_date": str(row.CUR_DATE),
            "current_time": str(row.CUR_TIME),
        }

        log_line("IBMI", "INFO", f"ACTION db2_ping finished: {result}")
        return result
    finally:
        conn.close()


def db2_metrics():
    """
    IBM i DB2 action: grouped invoice status counts.
    """
    log_line("IBMI", "INFO", "ACTION db2_metrics started")
    data = get_status_metrics()

    result = {
        "ok": True,
        "action": "db2_metrics",
        "metrics": data,
    }

    log_line("IBMI", "INFO", f"ACTION db2_metrics finished: {result}")
    return result


def db2_list_invoices_action():
    """
    IBM i DB2 action: list latest invoices.
    """
    log_line("IBMI", "INFO", "ACTION db2_list_invoices started")
    items = list_invoices(limit=20)

    result = {
        "ok": True,
        "action": "db2_list_invoices",
        "count": len(items),
        "items": items,
    }

    log_line("IBMI", "INFO", f"ACTION db2_list_invoices finished: count={len(items)}")
    return result

    """
    IBM i machine-oriented action.
    Returns connection/environment info for the IBM i integration layer.
    """
    log_line("IBMI", "INFO", "ACTION machine_info started")

    conn = get_connection()
    try:
        cur = conn.cursor()
        row = cur.execute(
            "SELECT CURRENT DATE AS CUR_DATE, CURRENT TIME AS CUR_TIME FROM SYSIBM.SYSDUMMY1"
        ).fetchone()

        result = {
            "ok": True,
            "action": "machine_info",
            "schema": get_schema(),
            "db2_alive": True,
            "current_date": str(row.CUR_DATE),
            "current_time": str(row.CUR_TIME),
            "allowed_actions": [
                "db2_ping",
                "db2_metrics",
                "db2_list_invoices",
                "machine_info",
            ],
        }

        log_line("IBMI", "INFO", f"ACTION machine_info finished: {result}")
        return result
    finally:
        conn.close()


def machine_info():
    """
    IBM i machine-oriented action.
    Returns connection/environment info for the IBM i integration layer.
    """
    log_line("IBMI", "INFO", "ACTION machine_info started")

    conn = get_connection()
    try:
        cur = conn.cursor()
        row = cur.execute(
            "SELECT CURRENT DATE AS CUR_DATE, CURRENT TIME AS CUR_TIME FROM SYSIBM.SYSDUMMY1"
        ).fetchone()

        result = {
            "ok": True,
            "action": "machine_info",
            "schema": get_schema(),
            "db2_alive": True,
            "current_date": str(row.CUR_DATE),
            "current_time": str(row.CUR_TIME),
            "allowed_actions": [
                "db2_ping",
                "db2_metrics",
                "db2_list_invoices",
                "machine_info",
            ],
        }

        log_line("IBMI", "INFO", f"ACTION machine_info finished: {result}")
        return result
    finally:
        conn.close()


def run_action(action: str):
    """
    Whitelisted IBM i action dispatcher.
    """
    action_name = action.strip().lower()

    if action_name == "db2_ping":
        return db2_ping()

    if action_name == "db2_metrics":
        return db2_metrics()

    if action_name == "db2_list_invoices":
        return db2_list_invoices_action()

    if action_name == "machine_info":
        return machine_info()

    log_line("IBMI", "WARN", f"Unknown IBM i action requested: {action}")
    return {
        "ok": False,
        "error": "unknown action",
        "requested_action": action,
        "allowed_actions": [
            "db2_ping",
            "db2_metrics",
            "db2_list_invoices",
            "machine_info",
        ],
    }