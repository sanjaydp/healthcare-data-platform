from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower
from pyspark.sql.functions import lit
import sys


def normalize_column_name(name: str) -> str:
    return name.strip().lower().replace("-", "_").replace(" ", "_")


def clean_dataframe(df):
    for old_name in df.columns:
        new_name = normalize_column_name(old_name)
        if old_name != new_name:
            df = df.withColumnRenamed(old_name, new_name)

    for c in df.columns:
        df = df.withColumn(c, trim(col(c).cast("string")))

    df = df.withColumn("record_source", lit("s3_pyspark"))
    return df


def main(input_dir: str, output_dir: str):
    spark = (
        SparkSession.builder
        .appName("HealthcareDataCleaning")
        .getOrCreate()
    )

    files = [
        "patients.csv",
        "encounters.csv",
        "conditions.csv",
        "observations.csv",
    ]

    for filename in files:
        input_path = str(Path(input_dir) / filename)
        output_path = str(Path(output_dir) / filename.replace(".csv", ""))

        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(input_path)
        )

        df = clean_dataframe(df)

        (
            df.coalesce(1)
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(output_path)
        )

        df = df.withColumn("record_source", lit("s3_pyspark"))

        print(f"Cleaned file written to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise ValueError("Usage: python clean_healthcare_data.py <input_dir> <output_dir>")

    main(sys.argv[1], sys.argv[2])