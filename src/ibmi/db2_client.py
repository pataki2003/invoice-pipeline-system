import pyodbc

SCHEMA = "PATAKI221"

def get_connection():
    return pyodbc.connect(
        "DSN=PUB400;"
        "UID=PATAKI22;"
        "PWD=ysd-k9-w!8tMbrjW",
        autocommit=True
    )

def main():
    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute(f"""
            INSERT INTO {SCHEMA}.INVOICE_PIPELINE
            (ORIGINAL_NAME, STATUS, FINAL_NAME, CREATED_AT)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        """, ("invoice_test.pdf", "READY", "invoice_test.pdf"))

        print("Inserted 1 row into DB2.")

        cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.INVOICE_PIPELINE")
        print("Total rows:", cur.fetchone()[0])

    finally:
        conn.close()

if __name__ == "__main__":
    main()