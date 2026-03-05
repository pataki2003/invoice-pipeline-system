# Invoice Pipeline

Enterprise-style invoice processing pipeline with IBM i integration.

This project simulates a realistic backend integration system.

---

# Features

- File ingestion pipeline
- Invoice validation
- File routing
- SQLite indexing
- IBM i DB2 integration
- Admin dashboard
- Structured logging
- REST API

---

# Architecture


Dropzone → Pipeline → SQLite + DB2 → API → Dashboard


---

# Technology

Backend:

- Python
- FastAPI
- pyodbc
- sqlite3

Frontend:

- HTML
- JavaScript

Enterprise Integration:

- IBM i
- DB2
- ODBC

---

# Project Structure


src/
api.py
pipeline.py
logger.py
ibmi/

data/
dropzone_in
ready
rejected


---

# Running Pipeline


python -m src.pipeline


---

# Running API


uvicorn src.api:app --reload


---

# IBM i Integration

Server:


pub400.com


Driver:


IBM i Access ODBC Driver


Table:


PATAKI221.INVOICE_PIPELINE


---

# Dashboard

Open:


/admin


Features:

- invoice table
- status transitions
- metrics
- pipeline trigger

---

# Roadmap

- DB2 status updates
- queue workers
- Docker deployment
- authentication