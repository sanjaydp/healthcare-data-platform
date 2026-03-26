from pathlib import Path
import mimetypes
import os

import boto3

# -------- Config --------
S3_BUCKET = "healthcare-synthea"
LOCAL_BRONZE_ROOT = Path("/opt/project/data/bronze")
S3_PREFIX = "bronze"

# Optional: limit to a specific streaming folder
# Example: {"encounter_events"}
INCLUDE_FOLDERS = set()  # leave empty to upload all bronze subfolders


def should_upload_folder(folder_name: str) -> bool:
    return not INCLUDE_FOLDERS or folder_name in INCLUDE_FOLDERS


def upload_file(s3_client, local_file: Path, s3_key: str) -> None:
    content_type, _ = mimetypes.guess_type(str(local_file))
    extra_args = {}
    if content_type:
        extra_args["ContentType"] = content_type

    s3_client.upload_file(
        Filename=str(local_file),
        Bucket=S3_BUCKET,
        Key=s3_key,
        ExtraArgs=extra_args if extra_args else None,
    )
    print(f"Uploaded: {local_file} -> s3://{S3_BUCKET}/{s3_key}")


def main() -> None:
    if not LOCAL_BRONZE_ROOT.exists():
        raise FileNotFoundError(f"Bronze root not found: {LOCAL_BRONZE_ROOT}")

    s3_client = boto3.client("s3")
    uploaded_count = 0

    for subdir in LOCAL_BRONZE_ROOT.iterdir():
        if not subdir.is_dir():
            continue
        if not should_upload_folder(subdir.name):
            continue

        for local_file in subdir.rglob("*"):
            if not local_file.is_file():
                continue
            if any(part.startswith(".") for part in local_file.parts):
                continue
            if "_spark_metadata" in local_file.parts:
                continue

            relative_path = local_file.relative_to(LOCAL_BRONZE_ROOT)
            s3_key = f"{S3_PREFIX}/{relative_path.as_posix()}"
            upload_file(s3_client, local_file, s3_key)
            uploaded_count += 1

    print(f"Upload complete. Total files uploaded: {uploaded_count}")


if __name__ == "__main__":
    main()