# Healthcare Data Engineering & Analytics Platform

An end-to-end healthcare data platform built with Kafka, Airflow, AWS S3, Snowflake, dbt, Great Expectations, OpenLineage, Marquez, and GitHub Actions.

This project simulates a production-style modern data stack pipeline for healthcare data, covering ingestion, orchestration, transformation, data quality, lineage, observability, and CI/CD.

---

## Architecture

```mermaid
flowchart LR
    A["Synthea Healthcare Event Data"] --> B["Kafka Encounter Events"]
    B --> C["Airflow Orchestration"]

    C --> D["PySpark Consumer"]
    D --> E["AWS S3 Bronze Layer"]

    E --> F["Snowflake Raw Layer"]
    F --> G["dbt Models Staging and Mart"]
    G --> H["dbt Tests"]

    E --> I["Great Expectations Data Validation"]
    I --> F

    G --> J["Power BI Dashboard"]

    C --> K["OpenLineage"]
    G --> K
    H --> K
    K --> L["Marquez Lineage Observability"]

    M["GitHub Actions CI CD Validation"] --> C
    M --> G
    M --> H
```
---

## Project Overview

This pipeline processes healthcare encounter data through a layered architecture:

- Event-driven ingestion using Kafka
- Workflow orchestration using Apache Airflow
- Data processing using PySpark
- Raw data storage in AWS S3 (Bronze layer)
- Data warehousing using Snowflake
- Transformations using dbt
- Data quality validation using Great Expectations and dbt tests
- Data lineage tracking using OpenLineage and Marquez
- CI/CD validation using GitHub Actions
- Dashboarding using Power BI

---

## Tech Stack

- Python
- Apache Kafka
- Apache Airflow
- PySpark
- AWS S3
- Snowflake
- dbt
- Great Expectations
- OpenLineage
- Marquez
- GitHub Actions
- Power BI

---

## Pipeline Flow

1. Healthcare encounter data is generated from source systems
2. Events are published into Kafka topics
3. Airflow orchestrates the pipeline execution
4. PySpark consumes Kafka events and processes them
5. Processed data is stored in AWS S3 (Bronze layer)
6. Great Expectations validates data quality
7. Clean data is loaded into Snowflake raw tables
8. dbt transforms raw data into staging and mart models
9. dbt tests validate model integrity
10. OpenLineage captures metadata from Airflow and dbt runs
11. Marquez visualizes lineage and pipeline dependencies
12. Power BI consumes curated datasets for analytics

---

## Key Features

- End-to-end data pipeline using modern data stack
- Event-driven ingestion with Kafka
- Orchestration with Apache Airflow
- Layered architecture (Bronze → Raw → Staging → Mart)
- Scalable transformations using dbt
- Data quality validation at multiple stages
- Lineage tracking with OpenLineage + Marquez
- CI/CD pipeline validation using GitHub Actions
- Analytics-ready datasets for reporting

---

## Data Layers

### Bronze Layer (AWS S3)
- Stores raw healthcare event data
- Immutable and source-aligned

### Raw Layer (Snowflake)
- Structured ingestion of raw data
- Minimal transformations applied

### Staging Layer (dbt)
- Data cleaning and standardization
- Schema alignment and transformations

### Mart Layer (dbt)
- Business-level aggregated datasets
- Analytics-ready tables

---

## Orchestration (Airflow)

Airflow manages the end-to-end pipeline:

- Kafka event ingestion
- Data processing with PySpark
- Data loading into S3 and Snowflake
- dbt model execution
- dbt testing
- Data quality validation
- Lineage tracking

Each stage is modular and independently executable.

---

## Data Quality

### Great Expectations
Used for:
- Schema validation
- Null checks
- Data consistency checks

### dbt Tests
Used for:
- Not null constraints
- Uniqueness checks
- Referential integrity

---

## Lineage & Observability

OpenLineage is integrated with Airflow and dbt to capture metadata across the pipeline.

Marquez provides visibility into:
- Job runs
- Task dependencies
- Upstream and downstream data flow
- Pipeline execution tracking

This helps in debugging, monitoring, and governance.

---

## CI/CD (GitHub Actions)

GitHub Actions is used to automate validation and deployment checks:

- DAG validation for Airflow
- dbt project validation
- Python dependency checks
- Docker build verification

This ensures reliability and consistency across deployments.

---

## Screenshots

### Airflow DAG Execution
<img width="1913" height="607" alt="DAG" src="https://github.com/user-attachments/assets/52141677-b3c7-4f72-bb78-00bca1bc16c2" />

### Airflow DAG Execution
<img width="1918" height="937" alt="mar" src="https://github.com/user-attachments/assets/45a19470-31a9-4c2f-b5ad-ce37584f04c5" />

### Power BI Dashboard - Executive Overview
<img width="1429" height="867" alt="image" src="https://github.com/user-attachments/assets/e1b9fc55-35ff-48a9-82e8-ca4db6917c31" />

### Power BI Dashboard - Condition Prevalence
<img width="1436" height="870" alt="image" src="https://github.com/user-attachments/assets/5f32e00f-f887-4082-8373-36a3265bae6e" />

### Power BI Dashboard - Condition Prevalence
<img width="1435" height="865" alt="image" src="https://github.com/user-attachments/assets/dfd5112f-3442-414a-8681-88247aca3c8e" />


---

## How to Run

### 1. Clone Repository
```bash
git clone https://github.com/sanjaydp/healthcare-data-platform.git
cd healthcare-data-platform

## Repository Structure

- `airflow/` - DAGs, logs, plugins, dbt profiles
- `pyspark/` - streaming and batch processing scripts
- `dbt/healthcare_dbt/` - dbt models, tests, and project files
- `great_expectations/` - data validation configuration
- `scripts/` - utility and pipeline execution scripts
- `data/` - sample and intermediate data assets
- `.github/workflows/` - CI/CD pipelines


## Repository Structure

- `airflow/` - DAGs, logs, plugins, dbt profiles
- `pyspark/` - streaming and batch processing scripts
- `dbt/healthcare_dbt/` - dbt models, tests, and project files
- `great_expectations/` - data validation configuration
- `scripts/` - utility and pipeline execution scripts
- `data/` - sample and intermediate data assets
- `.github/workflows/` - CI/CD pipelines
