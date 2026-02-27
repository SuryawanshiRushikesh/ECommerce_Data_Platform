# End-to-End Data Engineering Pipeline — E-Commerce Platform

A production-style Spark-based data engineering pipeline implementing the **Medallion Architecture (Bronze → Silver → Gold)** with data quality validation, orchestration, logging, and both business and ML data serving layers.

This project simulates a real-world e-commerce data platform handling user behavior events, orders, payments, business analytics, and ML feature engineering.

---

## Architecture Overview
Synthetic Data Generator  (Kafka Equivalent)
↓
Bronze Layer           (Raw JSONL — Data Lake)
↓
Spark Transformations
↓
Silver Layer           (Cleaned & Partitioned Parquet)
↓
Data Quality Validation
↓
Gold Layer
├── Business Metrics Tables
└── ML Feature Tables
↓
Pipeline Orchestration + Logging

This architecture mirrors modern enterprise data platforms and is designed for production evolution.

---

## Medallion Architecture

### Bronze Layer — Raw Data
- JSONL-based ingestion, immutable raw storage
- Simulates Kafka-style event streaming
- No transformations applied — source of truth

| Table | Description |
|---|---|
| `user_events` | Clickstream events — views, cart, checkout, purchase |
| `orders` | Order records linked to purchase events via `order_id` |
| `payments` | Payment transactions linked to orders via `order_id` |

---

### Silver Layer — Cleaned & Structured
- Schema enforcement and explicit type casting
- Null filtering and deduplication
- Domain-level validation (financial integrity, status whitelisting)
- Partitioned Parquet storage by date

| Table | Key Validations |
|---|---|
| `user_events` | Null checks, `event_type` normalization, `order_id` cast |
| `orders` | `total_amount == price × quantity`, valid status whitelist |
| `payments` | Amount sign validation per status, currency whitelist |

---

### Gold Layer — Analytics & ML Ready

#### Business Tables (Executive / BA / DA)

| Table | Description |
|---|---|
| `events_daily` | Daily funnel metrics, conversion rates, device & source split |
| `orders_daily` | Daily revenue, AOV, cancellation rate, unique buyers |
| `payments_daily` | Daily net revenue, refund rate, failure rate, method breakdown |
| `daily_business_metrics` | Master joined table — all KPIs in one place for dashboards |

#### ML Feature Tables (ML Engineers)

| Table | Description |
|---|---|
| `user_daily_features` | Per-user per-day feature vectors — behavior, transaction, payment signals |

Features include: `view_to_cart_rate`, `cart_to_checkout_rate`, `cart_abandon_flag`, `is_buyer_flag`, `has_failed_payment_flag`, `high_value_user_flag`, and more.

---

## Tech Stack

| Component | Technology |
|---|---|
| Processing | PySpark / Spark SQL |
| Storage | Parquet (columnar, partitioned) |
| Ingestion | Python data generators |
| Orchestration | Custom Python orchestrator with retry + logging |
| Environment | Python 3.x, Linux |
| Version Control | Git / GitHub |

**Production equivalents this maps to:**

| This Project | Production Equivalent |
|---|---|
| Data generators | Apache Kafka |
| Local disk storage | S3 / ADLS / GCS |
| Custom orchestrator | Apache Airflow |
| Local Spark | Spark on EMR / Databricks / Dataproc |

---

## Project Structure
ECommerce_Data_Platform/
│
├── ingestion/                  # Synthetic data generators
│   ├── event_generator.py      # User events (Bronze)
│   ├── orders_generator.py     # Orders (Bronze)
│   └── payments_generator.py   # Payments (Bronze)
│
├── spark_jobs/                 # Spark ETL pipelines
│   ├── bronze_to_silver_users.py
│   ├── bronze_to_silver_orders.py
│   ├── bronze_to_silver_payments.py
│   ├── gold_events_daily.py
│   ├── gold_orders_daily.py
│   ├── gold_payments_daily.py
│   ├── gold_daily_business_metrics.py
│   └── gold_user_daily_features.py
│
├── data_quality/               # Data validation checks
│   ├── check_user_events.py
│   ├── check_orders.py
│   └── check_payments.py
│
├── data/
│   ├── bronze/                 # Raw JSONL files
│   ├── silver/                 # Cleaned Parquet (partitioned by date)
│   └── gold/                   # Analytics & ML ready tables
│
├── logs/                       # Timestamped pipeline execution logs
└── orchestrator.py             # End-to-end pipeline runner

---

## How to Run

### 1. Setup Environment

Ensure Python 3.x and Apache Spark are installed and available on your `PATH`.
```bash
pip install pyspark
```

### 2. Run Full Pipeline
```bash
python orchestrator.py
```

### Pipeline Execution Order

Generate synthetic data          (Bronze JSONL)
Transform Bronze → Silver        (Cleaned Parquet)
Run Data Quality checks          (Validation layer)
Build intermediate Gold tables   (events, orders, payments daily)
Build master Gold tables         (business metrics + ML features)
Log execution metrics            (logs/ directory)


### 3. Run Individual Steps
```bash
# Ingestion
python ingestion/event_generator.py
python ingestion/orders_generator.py
python ingestion/payments_generator.py

# Bronze → Silver
spark-submit spark_jobs/bronze_to_silver_users.py
spark-submit spark_jobs/bronze_to_silver_orders.py
spark-submit spark_jobs/bronze_to_silver_payments.py

# Gold
spark-submit spark_jobs/gold_daily_business_metrics.py
spark-submit spark_jobs/gold_user_daily_features.py
```

---

## Pipeline Characteristics

- Full end-to-end execution logging with timestamped log files
- Retry mechanism for failed steps (configurable via `MAX_RETRIES`)
- Partitioned storage strategy across all layers
- Schema-controlled transformations with explicit column selection
- Business-rule validation logic (financial integrity, status whitelisting)
- ML-ready feature engineering with binary flags and ratio features
- Average full pipeline runtime: **~4–5 minutes** on local Spark

---

## Scalability Design

This pipeline is built to evolve. Scaling paths:

- Replace generators with **Kafka** for real-time ingestion
- Migrate storage to **S3 / ADLS / GCS** for cloud data lake
- Run Spark on **EMR / Databricks / Dataproc** for distributed compute
- Replace orchestrator with **Apache Airflow** for scheduling and monitoring
- Add **incremental partition-based processing** to avoid full reprocessing

---

## Key Engineering Concepts Demonstrated

- Medallion data modeling (Bronze / Silver / Gold)
- ETL pipeline design with PySpark
- Distributed processing and partitioned storage
- Data quality validation framework
- Pipeline orchestration with retry strategy
- Business KPI modeling for executive reporting
- ML feature engineering pipeline

---

## Future Enhancements

- Incremental / CDC-based processing
- Streaming support via Spark Structured Streaming
- Cloud deployment (AWS / Azure / GCP)
- Dashboard integration (Power BI / Tableau / Metabase)
- Data governance and metadata catalog (Apache Atlas / Unity Catalog)

---

## Summary

This project demonstrates the design and implementation of a production-style data engineering platform using PySpark and Python. It separates raw ingestion, transformation, validation, analytics serving, and ML feature engineering into clean, modular layers — following industry best practices for scalable data platform architecture.
