"""
ingestion/upload_to_gcs.py

Uploads the locally downloaded IBM AML dataset files to the Bronze GCS bucket.
Files are stored under:
  gs://<GCS_BRONZE_BUCKET>/raw/<filename>

Authentication: set GOOGLE_APPLICATION_CREDENTIALS to your service account JSON path.
"""

import os
import logging
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

GCS_BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET", "anti-ml-data-engineering-bronze")
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", "/tmp/aml_raw"))
GCS_PREFIX = "raw"


def upload_directory_to_gcs(
    local_dir: Path = DOWNLOAD_DIR,
    bucket_name: str = GCS_BRONZE_BUCKET,
    gcs_prefix: str = GCS_PREFIX,
) -> None:
    """Upload all files in local_dir to GCS bucket under gcs_prefix/."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    files = [f for f in local_dir.iterdir() if f.is_file()]
    if not files:
        logger.warning("No files found in '%s'. Nothing to upload.", local_dir)
        return

    for local_file in files:
        blob_name = f"{gcs_prefix}/{local_file.name}"
        blob = bucket.blob(blob_name)
        logger.info("Uploading '%s' → gs://%s/%s", local_file, bucket_name, blob_name)
        blob.upload_from_filename(str(local_file))
        logger.info("Upload complete: gs://%s/%s", bucket_name, blob_name)


if __name__ == "__main__":
    upload_directory_to_gcs()
