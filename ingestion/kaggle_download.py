"""
ingestion/kaggle_download.py

Downloads the IBM Anti Money Laundering dataset from Kaggle and saves it locally.
The Kaggle API credentials must be set via environment variables:
  - KAGGLE_USERNAME
  - KAGGLE_KEY
or via a kaggle.json file in the default Kaggle config directory (~/.kaggle/).
"""

import os
import zipfile
import logging
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

KAGGLE_DATASET = os.getenv("KAGGLE_DATASET", "ealtman2019/ibm-transactions-for-anti-money-laundering-aml")
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", "/tmp/aml_raw"))


def download_dataset(dataset: str = KAGGLE_DATASET, output_dir: Path = DOWNLOAD_DIR) -> Path:
    """Download a Kaggle dataset and unzip it into output_dir."""
    # Set Kaggle credentials from environment variables if provided
    kaggle_username = os.getenv("KAGGLE_USERNAME")
    kaggle_key = os.getenv("KAGGLE_KEY")
    if kaggle_username and kaggle_key:
        os.environ["KAGGLE_USERNAME"] = kaggle_username
        os.environ["KAGGLE_KEY"] = kaggle_key

    # Import here so credentials are set before kaggle reads them
    from kaggle.api.kaggle_api_extended import KaggleApiExtended

    output_dir.mkdir(parents=True, exist_ok=True)

    api = KaggleApiExtended()
    api.authenticate()

    logger.info("Downloading dataset '%s' to '%s'", dataset, output_dir)
    api.dataset_download_files(dataset, path=str(output_dir), unzip=False)

    # Unzip downloaded archive(s)
    zip_files = list(output_dir.glob("*.zip"))
    for zip_path in zip_files:
        logger.info("Unzipping '%s'", zip_path)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(output_dir)
        zip_path.unlink()

    logger.info("Download complete. Files in '%s': %s", output_dir, list(output_dir.iterdir()))
    return output_dir


if __name__ == "__main__":
    download_dataset()
