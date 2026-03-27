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
