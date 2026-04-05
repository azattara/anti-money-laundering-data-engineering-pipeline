"""
ingestion/kaggle_download.py

Downloads the IBM Anti Money Laundering dataset from Kaggle and streams it
directly to the GCS Bronze bucket.

The zip is downloaded to a temporary file on disk (not in memory) to avoid RAM
issues with large datasets. Each CSV inside the archive is then extracted and
streamed directly to GCS via blob.upload_from_file(). The temp file is deleted
automatically when done.

Environment variables:
  - KAGGLE_USERNAME / KAGGLE_KEY  — Kaggle API credentials
  - GCS_BRONZE_BUCKET             — Target GCS bucket (default: anti-ml-data-engineering-bronze)
  - GCS_PREFIX                    — Object prefix (default: raw)
  - GOOGLE_APPLICATION_CREDENTIALS — Path to GCP service account JSON
"""

import os
import tempfile
import zipfile
import logging

from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

KAGGLE_DATASET = os.getenv("KAGGLE_DATASET", "ealtman2019/ibm-transactions-for-anti-money-laundering-aml")
GCS_BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET", "anti-ml-data-engineering-bronze")
GCS_PREFIX = os.getenv("GCS_PREFIX", "raw")


def _download_dataset_to_tempfile(dataset: str) -> str:
    """Download the Kaggle dataset zip to a temporary file on disk. Returns the file path."""
    kaggle_username = os.getenv("KAGGLE_USERNAME")
    kaggle_key = os.getenv("KAGGLE_KEY")
    if kaggle_username and kaggle_key:
        os.environ["KAGGLE_USERNAME"] = kaggle_username
        os.environ["KAGGLE_KEY"] = kaggle_key

    from kaggle.api.kaggle_api_extended import KaggleApi
    from kagglesdk.datasets.types.dataset_api_service import ApiDownloadDatasetRequest

    api = KaggleApi()
    api.authenticate()

    owner_slug, dataset_slug, version = api.split_dataset_string(dataset)

    with api.build_kaggle_client() as kaggle:
        request = ApiDownloadDatasetRequest()
        request.owner_slug = owner_slug
        request.dataset_slug = dataset_slug
        request.dataset_version_number = int(version) if version else None
        response = kaggle.datasets.dataset_api_client.download_dataset(request)

        size = int(response.headers.get("Content-Length", 0))
        logger.info("Downloading zip to temp file (%.1f MB)…", size / 1e6)

        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".zip", prefix="aml_")
        chunk_size = 8 * 1024 * 1024  # 8 MB
        downloaded = 0
        for chunk in response.iter_content(chunk_size=chunk_size):
            tmp.write(chunk)
            downloaded += len(chunk)
            if size and downloaded % (chunk_size * 50) < chunk_size:
                logger.info("  %.0f%% (%.0f / %.0f MB)", downloaded / size * 100, downloaded / 1e6, size / 1e6)
        tmp.close()

    logger.info("Download complete → %s (%.1f MB)", tmp.name, downloaded / 1e6)
    return tmp.name


def download_and_stream_to_gcs(
    dataset: str = KAGGLE_DATASET,
    bucket_name: str = GCS_BRONZE_BUCKET,
    gcs_prefix: str = GCS_PREFIX,
) -> None:
    """Download Kaggle dataset zip to disk, then stream each file to GCS."""
    zip_path = _download_dataset_to_tempfile(dataset)

    try:
        gcs_client = storage.Client()
        bucket = gcs_client.bucket(bucket_name)
        uploaded = 0

        with zipfile.ZipFile(zip_path) as zf:
            entries = [e for e in zf.infolist() if not e.is_dir()]
            logger.info("Zip contains %d file(s). Streaming to GCS…", len(entries))

            for i, entry in enumerate(entries, 1):
                blob_name = f"{gcs_prefix}/{entry.filename}"
                blob = bucket.blob(blob_name)
                with zf.open(entry) as src:
                    logger.info("[%d/%d] Streaming '%s' (%.1f MB) → gs://%s/%s",
                                i, len(entries), entry.filename, entry.file_size / 1e6, bucket_name, blob_name)
                    blob.upload_from_file(src, content_type="text/csv")
                uploaded += 1

        logger.info("Done — %d file(s) streamed to gs://%s/%s/", uploaded, bucket_name, gcs_prefix)
    finally:
        os.unlink(zip_path)
        logger.info("Temp file deleted: %s", zip_path)


if __name__ == "__main__":
    download_and_stream_to_gcs()
