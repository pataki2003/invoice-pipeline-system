# Invoice Pipeline System

A small backend system that simulates an enterprise-style invoice processing workflow.

The project processes incoming invoice files, stores them in a database, exposes a REST API for status management, and provides an admin dashboard for manual control.

This project is also designed as preparation for IBM i (PUB400) integration.

---

# Features

File processing pipeline

- scans incoming files
- validates invoice naming format
- moves files to READY or REJECTED folders
- stores records in a database

REST API

- list invoices
- update invoice status
- view status history
- view metrics
- run pipeline from API

Admin dashboard

- view invoices
- filter by status
- update status
- view history
- run pipeline

Logging

- centralized logging
- request tracing using REQ IDs
- logs API, pipeline, and repository actions

IBM i preparation

- ODBC connection stub
- DB2 ping endpoint

---

# Tech Stack

Backend

Python  
FastAPI  
SQLite  
Uvicorn  

Frontend

HTML  
JavaScript  

Database

SQLite

Future integration

IBM i (PUB400)  
DB2 for i via ODBC

---

# Project Structure
src/
api.py
pipeline.py
logger.py

services/
invoice_service.py

repositories/
invoice_repository.py

ibmi/
db2_client.py

data/
dropzone_in
ready
rejected
index.db

logs/
---

# Running the API

Start server:
python -m uvicorn src.api


---

# Running the Pipeline


python src/pipeline.py


Processes files from:


data/dropzone_in


---

# Admin Dashboard

Simple HTML dashboard for managing invoices.

Features:

- view invoices
- change invoice status
- view metrics
- view history
- run pipeline

---

# Future Work

- IBM i DB2 integration
- ODBC DSN configuration
- invoice synchronization with DB2
- improved UI
- authentication