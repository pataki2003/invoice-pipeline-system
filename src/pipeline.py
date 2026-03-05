from pathlib import Path
from datetime import datetime
import re
import sqlite3
import os

from .logger import log_line, set_request_id
from .ibmi.db2_writer import insert_invoice as db2_insert_invoice

BASE_DIR = Path(__file__).resolve().parents[1]

DROPZONE = BASE_DIR / "data" / "dropzone_in"
READY = BASE_DIR / "data" / "ready"
REJECTED = BASE_DIR / "data" / "rejected"
LOGS_DIR = BASE_DIR / "logs"

DB_PATH = BASE_DIR / "data" / "index.db"

VALID_PATTERN = re.compile(r"^invoice_(\d+)\.pdf$", re.IGNORECASE)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS invoices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    original_name TEXT NOT NULL,
    status TEXT NOT NULL,
    final_name TEXT NOT NULL
);
"""

CREATE_UNIQUE_INDEX_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS ux_invoices_natural_key
ON invoices(timestamp, original_name, status, final_name);
"""

INSERT_SQL = """
INSERT OR IGNORE INTO invoices (timestamp, original_name, status, final_name)
VALUES (?, ?, ?, ?);
"""


def ensure_folders():
    for folder in [DROPZONE, READY, REJECTED, LOGS_DIR]:
        folder.mkdir(parents=True, exist_ok=True)


def init_db(conn: sqlite3.Connection):
    conn.execute(CREATE_TABLE_SQL)
    conn.execute(CREATE_UNIQUE_INDEX_SQL)
    conn.commit()


def classify(filename: str) -> str:
    return "READY" if VALID_PATTERN.match(filename) else "REJECTED"


def move_file(path: Path, destination_folder: Path) -> Path:
    target = destination_folder / path.name

    if target.exists():
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        target = destination_folder / f"{path.stem}__DUPLICATE__{stamp}{path.suffix}"

    path.rename(target)
    return target


def write_db(conn: sqlite3.Connection, ts: str, original: str, status: str, final: str) -> int:
    cur = conn.execute(INSERT_SQL, (ts, original, status, final))
    conn.commit()
    return cur.rowcount  # 1 inserted, 0 ignored


def process_dropzone():
    files = [p for p in DROPZONE.iterdir() if p.is_file()]

    if not files:
        print("No files in dropzone")
        log_line("PIPELINE", "INFO", "No files found in dropzone.")
        return

    log_line("PIPELINE", "INFO", f"Found {len(files)} file(s) in dropzone.")

    conn = sqlite3.connect(DB_PATH)
    try:
        init_db(conn)

        for f in files:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            status = classify(f.name)
            dest_folder = READY if status == "READY" else REJECTED

            try:
                dest = move_file(f, dest_folder)
                inserted = write_db(conn, ts, f.name, status, dest.name)

                db_msg = "DB+1" if inserted == 1 else "DB=0(dup)"
                print(f"{status:<8}| {f.name} -> {dest.name} | {db_msg}")
                log_line("PIPELINE", "INFO", f"{status} {f.name} -> {dest.name} ({db_msg})")
                
                try:
                    db2_insert_invoice(original_name=f.name, status=status, final_name=dest.name, created_at=ts)
                except Exception as e:
                    log_line("IBMI", "ERROR", f"DB2 insert failed for {f.name}: {e}")

            except Exception as e:
                # Ha valami nagyon félremegy egy fájllal, ne álljon le az egész futás
                print(f"ERROR   | {f.name} | {e}")
                log_line("PIPELINE", "ERROR", f"Failed processing {f.name}: {e}")

    finally:
        conn.close()


def main():
    rid = os.environ.get("REQ_ID", "-")
    set_request_id(rid)
    ensure_folders()

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] Pipeline started")
    log_line("PIPELINE", "INFO", "Pipeline started.")
    log_line("PIPELINE", "INFO", f"DB: {DB_PATH}")

    process_dropzone()


if __name__ == "__main__":
    main()