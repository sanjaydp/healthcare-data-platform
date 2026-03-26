FROM apache/airflow:2.10.5

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

RUN pip install --no-cache-dir \
    boto3 \
    dbt-core \
    dbt-snowflake \
    snowflake-connector-python \
    pandas \
    pyspark \
    great_expectations==0.18.21 \
    kafka-python