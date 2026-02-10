# 📘 Enterprise Data Cleaning & ETL Orchestration Framework using Python

## 📌 Project Overview

This project implements an **Enterprise-level ETL (Extract, Transform, Load) framework** using **Apache Airflow, Python, MySQL, and Docker**.

The system is designed to:
- Ingest large datasets
- Clean and standardize data
- Apply transformations
- Manage historical changes
- Monitor execution using metadata and dashboards

The pipeline follows a **layered data architecture**:

- **RAW Layer** – Stores ingested source data  
- **STAGING Layer** – Cleansed and standardized data  
- **CURATED Layer** – Business-ready, deduplicated, and validated data  

The framework is fully automated using **Airflow DAGs** and optimized for **large-scale data processing**.

---

## 🎯 Objectives

- Build a scalable ETL pipeline using Apache Airflow  
- Implement data cleaning and standardization techniques  
- Handle large datasets efficiently  
- Maintain data quality and validation checks  
- Track pipeline execution using metadata logging  
- Optimize performance using parallelism and batch processing  
- Provide visibility through dashboards and logs  

---

## 🏗️ System Architecture
```text
Source CSV / API
        ↓
     RAW Layer
        ↓
   STAGING Layer
        ↓
   CURATED Layer
        ↓
 Validation & Metadata
        ↓
 Dashboard & Reports
```
* Airflow DAGs orchestrate the entire pipeline
* MySQL stores data and metadata
* Docker ensures consistent environment setup
* Python handles transformation logic

## 🛠️ Tech Stack

* Apache Airflow 2.8+
* Python 3.8+
* MySQL
* Docker & Docker Compose
* Streamlit (Dashboard)
* Pandas & SQL
* Git & GitHub

# 🚀 Sprint-wise Implementation Summary
### Sprint 1 – Environment Setup & Pipeline Design

* Airflow installation using Docker
* DAG structure design
* Database schema creation (RAW, STAGING, CURATED)

### Sprint 2 – Data Cleaning & SCD Implementation

* Lookup table creation
* Standardization (uppercase/lowercase)
* Fuzzy matching for category mapping
* Slowly Changing Dimension (Type-2) logic
### Sprint 3 – SQL Transformations

* SQL-based transformations
* Validation of row counts
* Modular transformation scripts
### Sprint 4 – API Ingestion

* External API extraction
* Pagination and retry handling
* JSON storage in RAW layer

### Sprint 5 – Metadata & Auditing

* DAG run logging
* Task-level statistics
* Pipeline history tracking
* Metadata query APIs

### Sprint 6 – Version Control

* Dataset versioning
* Script and schema versioning
* Rollback strategy

### Sprint 7 – Dashboard & Monitoring

* Streamlit dashboard
* Execution history view
* Data quality score visualization
* Trend analysis charts

### Sprint 8 – Performance Optimization

* Airflow parallelism tuning
* DAG scheduling optimization
* Batch processing for large datasets
* Database query tuning
* I/O optimization

## 📁 Project Structure

```

Airflow/
│
├── docker/
│   ├── docker-compose.yaml          # Docker setup for Airflow, MySQL, Redis
│   └── .env                         # Environment variables
│
├── dags/
│   ├── amazon_etl_pipeline.py       # Main CSV-based ETL pipeline DAG
│   ├── amazon_sql_scheduler_dag.py  # SQL-based transformation scheduler DAG
│   └── api_pipeline_dag.py          # External API ingestion DAG
│
├── scripts/
│   ├── __init__.py                  # Marks scripts as a Python package
│   ├── csv_to_raw.py                # CSV → RAW ingestion
│   ├── raw_to_staging.py            # RAW → STAGING transformation
│   ├── staging_to_curated.py        # STAGING → CURATED transformation
│   ├── validate_pipeline.py         # End-to-end pipeline validation
│   ├── data_quality_checks.py       # Column-level data quality checks
│   ├── scd_category.py              # SCD Type-2 logic for dimensions
│   ├── fuzzy_mapper.py              # Fuzzy matching for category standardization
│   ├── metadata_logger.py           # Logs DAG & task metadata
│   ├── metadata_api.py              # APIs to expose pipeline metadata
│   ├── api_ingestion.py             # Extract data from external APIs
│   ├── api_clean_transform.py       # Clean & normalize API data
│   ├── api_load_to_mysql.py         # Load API data into MySQL
│   ├── run_sql_file.py              # Execute versioned SQL scripts
│   ├── schema_versioning.py         # Schema version control
│   ├── transform_versioning.py      # Transformation version tracking
│   └── rollback_transform.py        # Rollback mechanism for failed transforms
│
├── dashboard/
│   ├── app.py                       # Streamlit dashboard entry point
│   ├── api_client.py                # API client for dashboard
│   └── charts.py                   # Reusable chart components
│
├── data_models/
│   ├── raw_schema.sql               # RAW layer table definitions
│   ├── staging_schema.sql           # STAGING layer table definitions
│   ├── curated_schema.sql           # CURATED layer table definitions
│   ├── lookup_tables.sql            # Lookup & reference tables
│   └── metadata_tables.sql          # Metadata & audit tables
│
├── data/
│   ├── raw/                         # Raw source files (CSV / API dumps)
│   ├── staging/                     # Intermediate transformed data
│   └── processed/                   # Final processed datasets
│
├── logs/
│   └── airflow/                     # Airflow execution logs
│
├── docs/
│   ├── ETL Documentation
│
├── README.md                        # Project documentation
├── LICENSE                          # MIT License
└── requirements.txt                 # Python dependencies
```
## 🚀 Setup & Installation

### 1️⃣ Prerequisites

Make sure the following are installed:
- Docker
- Docker Compose
- Git
---

### 2️⃣ Clone the Repository
```bash
git clone https://github.com/your-username/Enterprise_ETL_Framework.git
cd Enterprise_ETL_Framework
```
### 3️⃣ Configure Environment Variables

Create a `.env` file in the project root directory:

```env
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

AIRFLOW_UID=50000
AIRFLOW__CORE__FERNET_KEY=your_fernet_key
AIRFLOW__WEBSERVER__SECRET_KEY=your_secret_key
```
## 4️⃣ Start Airflow Using Docker

```bash
docker compose up -d --build
```
### 5️⃣ Access Airflow UI
Open your browser and navigate to:
```bash
http://localhost:8080
```
---
* Default Credentials
* Username: airflow
* Password: airflow

### 6️⃣ Trigger the ETL Pipeline

* Open the DAGs page in the Airflow UI
* Enable the DAG amazon_etl_pipeline
* Trigger the DAG manually
### 7️⃣ Access the Dashboard
Run the Streamlit dashboard using:
```bash
docker compose exec webserver streamlit run /opt/airflow/dashboard/app.py

Then open:
http://localhost:8501
```
## 📊Key Features

* ✅ Layered architecture (RAW → STAGING → CURATED)
* ✅ Incremental & duplicate-safe ingestion
* ✅ SCD Type-2 implementation
* ✅ Metadata logging (DAG & task level)
* ✅ Dataset, script, and schema version control
* ✅ Data quality validation
* ✅ Dashboard with trends and DQ metrics
* ✅ Performance tuning for large datasets

## 🧪Validation & Quality Checks

* Row count consistency across layers
* Null checks on critical columns
* Duplicate detection
* DAG execution status validation

## ⚙️ Performance Optimization

* Airflow parallelism tuning
* Chunk-based batch processing
* Optimized SQL queries
* Controlled DAG concurrency
* Reduced memory footprint

## 📈 Dashboard Capabilities

* Latest pipeline status
* Pipeline run history
* Data volume trends
* Data quality indicators
* Status distribution charts

## 📌 Key Learnings

* Enterprise ETL architecture design
* Workflow orchestration using Airflow
* Data quality and governance
* Performance optimization techniques
* Production-level debugging and monitoring

## 📜License

This project is licensed under the MIT License. See `LICENSE`.
  
