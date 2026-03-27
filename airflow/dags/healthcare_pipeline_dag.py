from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "sanjay",
}

with DAG(
    dag_id="healthcare_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 2, 10),
    schedule=None,
    catchup=False,
    tags=["healthcare", "kafka", "snowflake", "dbt"],
) as dag:

    produce_encounter_events = BashOperator(
        task_id="produce_encounter_events",
        bash_command="""
        cd /opt/project/scripts && \
        python kafka_producer_encounters.py
        """,
    )

    consume_encounter_events = BashOperator(
        task_id="consume_encounter_events",
        bash_command="""
        cd /opt/project/pyspark && \
        timeout 60 python stream_encounters_from_kafka.py
        """,
    )

    upload_bronze_to_s3 = BashOperator(
        task_id="upload_bronze_to_s3",
        bash_command="""
        cd /opt/project/scripts && \
        python upload_bronze_to_s3.py
        """,
    )

    load_raw_to_snowflake = BashOperator(
        task_id="load_raw_to_snowflake",
        bash_command="""
        cd /opt/project/scripts && \
        python load_s3_pyspark_to_snowflake.py
        """,
    )

    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command="""
        docker exec dbt dbt run \
        --project-dir /usr/app/dbt/healthcare_dbt \
        --profiles-dir /usr/app/airflow/dbt_profiles
        """,
    )

    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command="""
        docker exec dbt dbt test \
        --project-dir /usr/app/dbt/healthcare_dbt \
        --profiles-dir /usr/app/airflow/dbt_profiles
        """,
    )

    produce_encounter_events >> consume_encounter_events >> upload_bronze_to_s3 >> load_raw_to_snowflake >> run_dbt_models >> run_dbt_tests