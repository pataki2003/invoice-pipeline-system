from pathlib import Path
from datetime import datetime, date
import sqlite3
from logger import log_line

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "index.db"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS invoices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    original_name TEXT NOT NULL,
    status TEXT NOT NULL,
    final_name TEXT NOT NULL
);
"""

# Duplikát törlés: megtartjuk a legkisebb id-t ugyanarra a "természetes kulcsra"
DEDUP_SQL = """
DELETE FROM invoices
WHERE id NOT IN (
    SELECT MIN(id)
    FROM invoices
    GROUP BY timestamp, original_name, status, final_name
);
"""

CREATE_UNIQUE_INDEX_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS ux_invoices_natural_key
ON invoices(timestamp, original_name, status, final_name);
"""

COUNT_SQL = "SELECT COUNT(*) FROM invoices;"




def init_db(conn: sqlite3.Connection):
    conn.execute(CREATE_TABLE_SQL)
    conn.commit()


def count_rows(conn: sqlite3.Connection) -> int:
    return conn.execute(COUNT_SQL).fetchone()[0]


def deduplicate(conn: sqlite3.Connection) -> int:
    before = count_rows(conn)
    conn.execute(DEDUP_SQL)
    conn.commit()
    after = count_rows(conn)
    return before - after


def ensure_unique_index(conn: sqlite3.Connection):
    conn.execute(CREATE_UNIQUE_INDEX_SQL)
    conn.commit()


def main():
    log_line("INDEX_DB", "INFO", f"index_db started. DB={DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    try:
        init_db(conn)
        log_line("INDEX_DB", "INFO", "Ensured table exists: invoices")

        removed = deduplicate(conn)
        log_line("INDEX_DB", "INFO", f"Duplicates removed: {removed}")

        ensure_unique_index(conn)
        log_line("INDEX_DB", "INFO", "Ensured unique index exists: ux_invoices_natural_key")

        total = count_rows(conn)
        log_line("INDEX_DB", "INFO", f"Total rows now: {total}")

        print(f"Duplicates removed: {removed}")
        print(f"Total rows in DB: {total}")

    finally:
        conn.close()
        log_line("INDEX_DB", "INFO", "index_db finished.")


if __name__ == "__main__":
    main()