from pathlib import Path
import tempfile
import subprocess
import shutil

import boto3
import pandas as pd
import snowflake.connector

SNOWFLAKE_CONFIG = {
    "user": "SanjayDP135",
    "password": "24Cwe:cJMZW9wN.",
    "account": "aoc68017.us-east-1",
    "warehouse": "COMPUTE_WH",
    "database": "HC_DB",
    "schema": "RAW",
    "role": "ACCOUNTADMIN",
}

S3_BUCKET = "healthcare-synthea"

S3_KEYS = {
    "patients.csv": "raw/patients/patients.csv",
    "encounters.csv": "raw/encounters/encounters.csv",
    "conditions.csv": "raw/conditions/conditions.csv",
    "observations.csv": "raw/observations/observations.csv",
}


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.astype(object)
    df = df.where(pd.notna(df), None)
    return df


def create_table_from_df(cur, table_name: str, df: pd.DataFrame):
    column_defs = ", ".join([f'"{col}" STRING' for col in df.columns])
    cur.execute(f"CREATE OR REPLACE TABLE {table_name} ({column_defs})")


def insert_df(cur, table_name: str, df: pd.DataFrame):
    cols = ", ".join([f'"{col}"' for col in df.columns])
    placeholders = ", ".join(["%s"] * len(df.columns))
    sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
    rows = [tuple(None if pd.isna(v) else str(v) for v in row) for row in df.to_numpy()]
    cur.executemany(sql, rows)


def find_single_csv(folder: Path) -> Path:
    csv_files = list(folder.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV file found in {folder}")
    return csv_files[0]


def load_cleaned_file(cur, cleaned_dir: Path, folder_name: str, table_name: str):
    csv_file = find_single_csv(cleaned_dir / folder_name)
    df = pd.read_csv(csv_file)
    df = clean_dataframe(df)
    create_table_from_df(cur, table_name, df)
    insert_df(cur, table_name, df)
    print(f"Loaded {len(df)} rows into {table_name}")


def main():
    s3 = boto3.client("s3")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        raw_dir = tmpdir / "raw"
        clean_dir = tmpdir / "clean"
        raw_dir.mkdir(parents=True, exist_ok=True)
        clean_dir.mkdir(parents=True, exist_ok=True)

        for filename, key in S3_KEYS.items():
            target_path = raw_dir / filename
            print(f"Downloading s3://{S3_BUCKET}/{key} -> {target_path}")
            s3.download_file(S3_BUCKET, key, str(target_path))

        subprocess.run(
            [
                "python",
                "/opt/project/pyspark/clean_healthcare_data.py",
                str(raw_dir),
                str(clean_dir),
            ],
            check=True,
        )

        subprocess.run(
            [
                "python",
                "/opt/project/great_expectations/validate_healthcare_data.py",
                str(clean_dir),
            ],
            check=True,
        )

        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cur = conn.cursor()

        try:
            cur.execute("USE WAREHOUSE COMPUTE_WH")
            cur.execute("USE DATABASE HC_DB")
            cur.execute("USE SCHEMA RAW")

            load_cleaned_file(cur, clean_dir, "patients", "HC_DB.RAW.PATIENTS")
            load_cleaned_file(cur, clean_dir, "encounters", "HC_DB.RAW.ENCOUNTERS")
            load_cleaned_file(cur, clean_dir, "conditions", "HC_DB.RAW.CONDITIONS")
            load_cleaned_file(cur, clean_dir, "observations", "HC_DB.RAW.OBSERVATIONS")

            conn.commit()
            print("S3 -> PySpark -> Snowflake completed successfully.")
        finally:
            cur.close()
            conn.close()


if __name__ == "__main__":
    main()