from pathlib import Path
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

BASE_DIR = Path("/opt/project/data/raw_sample")


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip().lower().replace("-", "_") for c in df.columns]
    return df


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


def load_file(cur, filename: str, table_name: str):
    df = pd.read_csv(BASE_DIR / filename)
    df = normalize_columns(df)
    df = clean_dataframe(df)
    create_table_from_df(cur, table_name, df)
    insert_df(cur, table_name, df)
    print(f"Loaded {len(df)} rows into {table_name}")


def main():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur = conn.cursor()
    try:
        cur.execute("USE WAREHOUSE COMPUTE_WH")
        cur.execute("USE DATABASE HC_DB")
        cur.execute("USE SCHEMA RAW")

        load_file(cur, "patients.csv", "HC_DB.RAW.PATIENTS")
        load_file(cur, "encounters.csv", "HC_DB.RAW.ENCOUNTERS")
        load_file(cur, "conditions.csv", "HC_DB.RAW.CONDITIONS")
        load_file(cur, "observations.csv", "HC_DB.RAW.OBSERVATIONS")

        conn.commit()
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()