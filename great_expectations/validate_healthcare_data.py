from pathlib import Path
import sys
import pandas as pd
from great_expectations.dataset import PandasDataset


def validate_patients(path: Path):
    df = PandasDataset(pd.read_csv(path))
    results = [
        df.expect_column_to_exist("id"),
        df.expect_column_values_to_not_be_null("id"),
        df.expect_column_values_to_be_unique("id"),
        df.expect_column_values_to_not_be_null("birthdate"),
    ]
    return all(r["success"] for r in results)


def validate_encounters(path: Path):
    df = PandasDataset(pd.read_csv(path))
    results = [
        df.expect_column_to_exist("id"),
        df.expect_column_values_to_not_be_null("id"),
        df.expect_column_values_to_be_unique("id"),
        df.expect_column_values_to_not_be_null("patient"),
    ]
    return all(r["success"] for r in results)


def validate_conditions(path: Path):
    df = PandasDataset(pd.read_csv(path))
    results = [
        df.expect_column_to_exist("patient"),
        df.expect_column_to_exist("code"),
        df.expect_column_values_to_not_be_null("patient"),
        df.expect_column_values_to_not_be_null("code"),
    ]
    return all(r["success"] for r in results)


def validate_observations(path: Path):
    df = PandasDataset(pd.read_csv(path))
    results = [
        df.expect_column_to_exist("patient"),
        df.expect_column_to_exist("code"),
        df.expect_column_values_to_not_be_null("patient"),
        df.expect_column_values_to_not_be_null("code"),
    ]
    return all(r["success"] for r in results)


def find_single_csv(folder: Path) -> Path:
    csv_files = list(folder.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV file found in {folder}")
    return csv_files[0]


def main(clean_dir: str):
    clean_path = Path(clean_dir)

    patients_ok = validate_patients(find_single_csv(clean_path / "patients"))
    encounters_ok = validate_encounters(find_single_csv(clean_path / "encounters"))
    conditions_ok = validate_conditions(find_single_csv(clean_path / "conditions"))
    observations_ok = validate_observations(find_single_csv(clean_path / "observations"))

    if not all([patients_ok, encounters_ok, conditions_ok, observations_ok]):
        raise ValueError("Great Expectations validation failed.")

    print("Great Expectations validation passed for all datasets.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise ValueError("Usage: python validate_healthcare_data.py <clean_dir>")
    main(sys.argv[1])