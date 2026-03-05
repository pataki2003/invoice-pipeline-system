from pathlib import Path
from datetime import datetime
import sqlite3
from fastapi import FastAPI, Query
from src.logger import log_line
from src.services.invoice_service import validate_status, validate_transition, build_history_record
from src.repositories.invoice_repository import (
    get_invoice as repo_get_invoice,
    list_invoices as repo_list_invoices,
    update_invoice_status as repo_update_invoice_status,
    get_status_metrics as repo_get_status_metrics,
)
from src.repositories.invoice_repository import (
    insert_status_history as repo_insert_status_history,
    list_status_history as repo_list_status_history,
)
import uuid
from fastapi import Request
from src.logger import set_request_id
from fastapi.responses import HTMLResponse
import os
from src.logger import get_request_id
import time
from src.ibmi.db2_client import ping as db2_ping


BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "index.db"
DASHBOARD_PATH = BASE_DIR / "src" / "web" / "dashboard.html"

app = FastAPI(title="Invoice Pipeline API")
@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    rid = uuid.uuid4().hex[:8]
    set_request_id(rid)

    log_line("API", "INFO", f"{request.method} {request.url.path}")

    response = await call_next(request)
    return response

CREATE_HISTORY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS invoice_status_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    invoice_id INTEGER NOT NULL,
    changed_at TEXT NOT NULL,
    old_status TEXT NOT NULL,
    new_status TEXT NOT NULL
);
"""


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(CREATE_HISTORY_TABLE_SQL)
        conn.commit()
        log_line("API", "INFO", "DB init complete (history table ensured)")
    finally:
        conn.close()


@app.on_event("startup")
def on_startup():
    init_db()


@app.get("/health")
def health():
    log_line("API", "INFO", "health check")
    return {"status": "ok"}


@app.get("/invoices")
def list_invoices(
    status: str | None = Query(default=None, description="Filter by status: READY or REJECTED"),
    limit: int = Query(default=50, ge=1, le=500),
):
    log_line("API", "INFO", f"list_invoices status={status} limit={limit}")

    conn = get_conn()
    try:
        rows = repo_list_invoices(conn, status.upper() if status else None, limit)
        return [dict(r) for r in rows]
    finally:
        conn.close()


@app.get("/invoices/{invoice_id}")
def get_invoice(invoice_id: int):
    log_line("API", "INFO", f"get_invoice id={invoice_id}")

    conn = get_conn()
    try:
        row = repo_get_invoice(conn, invoice_id)

        if row is None:
            log_line("API", "WARN", f"invoice not found id={invoice_id}")
            return {"error": "invoice not found"}

        return dict(row)
    finally:
        conn.close()



@app.patch("/invoices/{invoice_id}/status")
def update_status(invoice_id: int, status: str):
    new_status = status.upper()

    if not validate_status(new_status):
        log_line("API", "WARN", f"update_status invalid status={status} id={invoice_id}")
        return {"error": "invalid status"}

    log_line("API", "INFO", f"update_status id={invoice_id} -> {new_status}")

    conn = get_conn()
    try:
        current = conn.execute(
            "SELECT status FROM invoices WHERE id = ?",
            (invoice_id,),
        ).fetchone()

        if current is None:
            log_line("API", "WARN", f"update_status invoice not found id={invoice_id}")
            return {"error": "invoice not found"}

        old_status = current["status"]

        check = validate_transition(old_status, new_status)
        if not check["valid"]:
            log_line("API", "WARN", f"invalid transition {old_status} -> {new_status} invoice_id={invoice_id}")
            return {
                "error": f"invalid transition {old_status} -> {new_status}",
                "allowed": check["allowed"],
            }

        updated = repo_update_invoice_status(conn, invoice_id, new_status)
        if updated == 0:
            log_line("API", "WARN", f"update_status invoice not found id={invoice_id}")
            return {"error": "invoice not found"}

        h = build_history_record(invoice_id, old_status, new_status)
        repo_insert_status_history(conn, h["invoice_id"], h["changed_at"], h["old_status"], h["new_status"])

        row = repo_get_invoice(conn, invoice_id)

        return dict(row) if row else {"error": "invoice not found"}

    finally:
        conn.close()


@app.get("/invoices/{invoice_id}/history")
def invoice_history(invoice_id: int, limit: int = Query(default=50, ge=1, le=500)):
    log_line("API", "INFO", f"invoice_history id={invoice_id} limit={limit}")

    conn = get_conn()
    try:
        rows = repo_list_status_history(conn, invoice_id, limit)

        return [dict(r) for r in rows]
    finally:
        conn.close()

@app.get("/metrics")
def metrics():
    log_line("API", "INFO", "metrics")

    conn = get_conn()
    try:
        rows = repo_get_status_metrics(conn)

        # list -> dict: {"READY": 10, "REJECTED": 3, ...}
        return {r["status"]: r["count"] for r in rows}
    finally:
        conn.close()

@app.get("/admin", response_class=HTMLResponse)
def admin():
    log_line("API", "INFO", "admin dashboard")
    html = DASHBOARD_PATH.read_text(encoding="utf-8")
    return HTMLResponse(content=html)

import subprocess

@app.post("/ibmi/run")
def run_ibmi():
    log_line("API", "INFO", "ibmi run requested (stub)")

    # Itt majd később: PUB400 job submit / program call / DB2 query.
    # Most csak szimuláljuk, hogy “valami fut”.
    time.sleep(1)

    log_line("API", "INFO", "ibmi run finished (stub)")
    return {"status": "ok", "message": "IBM i job simulated"}

@app.post("/pipeline/run")
def run_pipeline():
    log_line("API", "INFO", "pipeline run requested")

    try:
        env = os.environ.copy()
        env["REQ_ID"] = get_request_id()

        result = subprocess.run(
            ["python", "src/pipeline.py"],
            capture_output=True,
            text=True,
            env=env,
        )

        log_line("API", "INFO", "pipeline run finished")

        return {
            "status": "ok",
            "stdout": result.stdout,
            "stderr": result.stderr,
        }

    except Exception as e:
        log_line("API", "ERROR", f"pipeline run failed: {e}")
        return {"error": str(e)}
    
@app.get("/ibmi/discovery")
def ibmi_discovery():
    log_line("API", "INFO", "ibmi discovery requested")

    return {
        "goal": "Decide how to integrate with PUB400 safely.",
        "recommended_path": [
            "Use DB2 for i via ODBC if you need to read/write IBM i data",
            "Use SSH/QSH only if enabled (often disabled on public systems)",
            "Use 5250 for manual validation and learning"
        ],
        "info_needed_from_you": [
            "PUB400 host/address (or URL if web based)",
            "Your user profile name (no password)",
            "Do you have 5250 access? (yes/no)",
            "Do you have DB2/ODBC access? (yes/no/unknown)",
            "Is SSH enabled? (yes/no/unknown)"
        ],
        "next_step": "Collect access details and choose ODBC or SSH approach."
    }

@app.get("/ibmi/db2/ping")
def ibmi_db2_ping():
    log_line("API", "INFO", "ibmi db2 ping requested")
    try:
        return db2_ping()
    except Exception as e:
        log_line("API", "ERROR", f"ibmi db2 ping failed: {e}")
        return {"error": str(e)}