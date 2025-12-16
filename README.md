# Paralex Data Pipeline (Airflow + dbt + Postgres)

> **Portfolio / showcase project**  
> This repository presents a *non-reproducible*, real-world data pipeline built to demonstrate tools, architecture, and engineering decisions.  
> Sensitive data, credentials, and research-specific outputs are intentionally excluded.

---

## Overview

This project implements an end-to-end data pipeline around **Paralex linguistic datasets**, orchestrated with **Apache Airflow**, stored in **PostgreSQL (Docker)**, and lightly modeled with **dbt**.  
The pipeline was developed in the context of an academic collaboration and is published here purely to showcase data engineering and analytics-engineering skills.

**Paralex** (https://paralex-standard.org):  
> *Paralex is a standard for morphological lexicons which document inflectional paradigms.*

---

## Tech stack

- **Apache Airflow** – orchestration and task dependency management
- **Python / Pandas** – data processing and feature expansion
- **PostgreSQL (Docker)** – analytical storage layer
- **dbt** – light transformations, ordering, and semantic cleanup
- **Docker / Docker Compose** – local infrastructure

---

## High-level data flow

```
┌──────────────┐
│   Internet   │
│ (Paralex)   │
└──────┬───────┘
       │ 1. Download (Airflow + paralex CLI)
       ▼
┌──────────────────────┐
│   Raw CSV datasets   │
│ (per language)       │
└──────┬───────────────┘
       │ 2. Transform
       │   - parse morphological codes
       │   - join feature dictionaries
       ▼
┌──────────────────────┐
│ Processed CSV files  │
│ (+ linguistic cols)  │
└──────┬───────────────┘
       │ 3. Ingest
       │   (Airflow → Postgres)
       ▼
┌──────────────────────┐
│ PostgreSQL database  │
│  raw / processed     │
│  language tables     │
└──────┬───────────────┘
       │ 4. dbt models
       │   - cleaning
       │   - ordering / ranking
       ▼
┌──────────────────────┐
│ Analytics-ready      │
│ tables               │
└─────────┬────────────┘
          │ 5. Python analysis
          ▼
┌──────────────────────┐
│ Research & analysis  │
│ (outside scope here) │
└──────────────────────┘
```

---

## Pipeline stages

### 1. Data acquisition

- Airflow DAG downloads linguistic datasets from the web using the **`paralex` Python/CLI package**
- Data is stored locally inside the Airflow container, organized by language
- One task per language, enabling parallel downloads

---

### 2. Linguistic feature expansion

- Each dataset includes a **feature dictionary** mapping compact morphological codes to linguistic categories
- A custom Python transformation:
  - Parses encoded strings (e.g. `prs.ind.1sg`)
  - Expands them into explicit linguistic columns (tense, mood, person, number, etc.)
- Output is written as `_processed.csv` files alongside raw data

---

### 3. Ingestion into PostgreSQL

- Airflow ingests both **raw** and **processed** CSVs into PostgreSQL tables
- One table per language and processing stage (e.g. `french_raw`, `french_processed`)
- Ingestion is idempotent and schema-driven

---

### 4. dbt modeling

- dbt is used for **light transformations**, not heavy reshaping
- Typical operations:
  - Column normalization
  - Ordering / ranking following conjugation paradigms
  - Semantic cleanup for analysis readiness

This layer establishes a clean analytical interface without duplicating upstream logic.

---

### 5. Downstream analysis

Once data is modeled in Postgres:
- Python-based analysis becomes straightforward
- Further statistical or linguistic exploration can be layered on top

(Downstream research analysis is intentionally **out of scope** for this repository.)

---

## Project structure (simplified)

```
airflow/
  dags/
    download_*.py
    transform_*.py
    ingest_*.py

paralex_project/
  models/
  macros/
  tests/
  dbt_project.yml

docker-compose.yml
dockerfile
```

---

## Notes

- This repository is **not intended to be executed as-is**
- It reflects a real production-like pipeline, with data, credentials, and research outputs removed
- The focus is on **architecture, orchestration, and transformation design**

---

## Author

Built by *Andrea Valente* as part of an academic data collaboration and published here as a professional portfolio project.

