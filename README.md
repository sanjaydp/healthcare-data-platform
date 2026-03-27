# Healthcare Data Engineering & Analytics Platform

An end-to-end healthcare data platform built using Kafka, Airflow, AWS S3, Snowflake, dbt, Great Expectations, OpenLineage, Marquez, and GitHub Actions.

This project was designed to simulate a production-style modern data stack pipeline for healthcare data, covering ingestion, orchestration, transformation, data quality, lineage, and CI/CD.

---

## Architecture

```mermaid
flowchart LR
    A[Synthea / Healthcare Event Data] --> B[Kafka<br/>Encounter Events]
    B --> C[Airflow Orchestration]

    C --> D[PySpark Consumer]
    D --> E[AWS S3 Bronze Layer]

    E --> F[Load to Snowflake Raw Layer]
    F --> G[dbt Models<br/>Staging / Mart Layer]
    G --> H[dbt Tests]

    E --> I[Great Expectations<br/>Data Validation]
    I --> F

    G --> J[Power BI Dashboard]

    C --> K[OpenLineage]
    G --> K
    H --> K
    K --> L[Marquez<br/>Lineage & Observability]

    M[GitHub Actions CI/CD] --> C
    M --> G
    M --> H

# Healthcare Data Engineering & Analytics Platform

An end-to-end healthcare data platform built using a modern data stack, covering ingestion, orchestration, transformation, data quality, lineage, and CI/CD.

This project simulates a production-style pipeline for healthcare data, designed to demonstrate how real-world data engineering systems are built and operated.

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



