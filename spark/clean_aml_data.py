"""
spark/clean_aml_data.py

PySpark job — Silver Layer (Incremental)

Reads IBM AML CSV files from the Bronze GCS bucket, applies cleaning and
validation, then writes results to BigQuery (aml_silver dataset) and to the
Silver GCS bucket.

Incremental mode (default)
  - Accepts --partition-date YYYY-MM-DD  OR  --partitions YYYY-MM-DD,YYYY-MM-DD
  - Reads only GCS paths matching bronze/<partition-date>/...
  - Writes with WRITE_APPEND so history is preserved.
  - Records processed partitions in the ops.ingestion_manifest BigQuery table to
    prevent double-processing.

Full-refresh mode
  - Pass --full-refresh to process all Bronze files (WRITE_TRUNCATE).

Connector: spark-bigquery-with-dependencies
  spark-submit \
    --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0 \
    spark/clean_aml_data.py [--full-refresh] [--partition-date YYYY-MM-DD]
"""

import argparse
import json
import os
import logging
from datetime import date, datetime, timedelta
from typing import List, Optional

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, TimestampType

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Configuration (overridable via environment variables)
# ─────────────────────────────────────────────────────────────────────────────
GCP_PROJECT_ID    = os.getenv("GCP_PROJECT_ID",       "anti-ml-data-engineering")
GCS_BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET",    "anti-ml-data-engineering-bronze")
GCS_SILVER_BUCKET = os.getenv("GCS_SILVER_BUCKET",    "anti-ml-data-engineering-silver")
BQ_SILVER_DATASET = os.getenv("BQ_SILVER_DATASET",    "aml_silver")
BQ_OPS_DATASET    = os.getenv("BQ_OPS_DATASET",       "aml_ops")
TEMP_GCS_BUCKET   = os.getenv("GCS_ARTIFACTS_BUCKET", "anti-ml-data-engineering-artifacts")

BRONZE_RAW_PREFIX = "raw"          # gs://<bronze>/raw/                (full-refresh)
BRONZE_DATE_PREFIX = "partitions"  # gs://<bronze>/partitions/YYYY-MM-DD/ (incremental)
SILVER_GCS_PATH   = f"gs://{GCS_SILVER_BUCKET}/cleaned/"
BQ_TABLE          = f"{GCP_PROJECT_ID}.{BQ_SILVER_DATASET}.transactions"
BQ_MANIFEST_TABLE = f"{GCP_PROJECT_ID}.{BQ_OPS_DATASET}.ingestion_manifest"


# ─────────────────────────────────────────────────────────────────────────────
# Spark session
# ─────────────────────────────────────────────────────────────────────────────
def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("AML-Silver-Layer")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────────────────────
# Manifest helpers (BigQuery-backed checkpoint)
# ─────────────────────────────────────────────────────────────────────────────
def get_processed_partitions(spark: SparkSession) -> List[str]:
    """Return list of partition_date strings already successfully processed."""
    try:
        df = (
            spark.read.format("bigquery")
            .option("table", BQ_MANIFEST_TABLE)
            .load()
        )
        rows = df.filter(F.col("status") == "SUCCESS").select("partition_date").collect()
        return [r["partition_date"] for r in rows]
    except Exception as exc:
        logger.warning("Could not read manifest (first run?): %s", exc)
        return []


def record_manifest(spark: SparkSession, partition_date: str, status: str, row_count: int) -> None:
    """Upsert a manifest record for the given partition_date."""
    schema = "partition_date STRING, status STRING, row_count LONG, processed_at TIMESTAMP"
    record = spark.createDataFrame(
        [(partition_date, status, row_count, datetime.utcnow())],
        schema=schema,
    )
    try:
        (
            record.write.format("bigquery")
            .option("table", BQ_MANIFEST_TABLE)
            .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
            .option("createDisposition", "CREATE_IF_NEEDED")
            .option("writeDisposition", "WRITE_APPEND")
            .save()
        )
        logger.info("Manifest updated: partition=%s status=%s rows=%d", partition_date, status, row_count)
    except Exception as exc:
        logger.warning("Could not write manifest: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# I/O helpers
# ─────────────────────────────────────────────────────────────────────────────
def read_bronze_full(spark: SparkSession) -> "pyspark.sql.DataFrame":
    """Read all CSV files from the Bronze bucket (full-refresh mode)."""
    path = f"gs://{GCS_BRONZE_BUCKET}/{BRONZE_RAW_PREFIX}/"
    logger.info("Full-refresh: reading Bronze from %s", path)
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
    )


def read_bronze_partitions(spark: SparkSession, partition_dates: List[str]) -> "pyspark.sql.DataFrame":
    """Read CSV files for specific date partitions (incremental mode)."""
    paths = [
        f"gs://{GCS_BRONZE_BUCKET}/{BRONZE_DATE_PREFIX}/{d}/"
        for d in partition_dates
    ]
    logger.info("Incremental: reading Bronze partitions %s", partition_dates)
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(paths)
    )


# ─────────────────────────────────────────────────────────────────────────────
# Cleaning
# ─────────────────────────────────────────────────────────────────────────────
def _resolve_column(df: "pyspark.sql.DataFrame", candidates: List[str]) -> Optional[str]:
    """Return the first candidate column name that exists in the DataFrame."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def clean_dataframe(df: "pyspark.sql.DataFrame") -> "pyspark.sql.DataFrame":
    """
    Apply Silver-layer cleaning transformations:
      1. Standardise column names (lowercase, spaces → underscores)
      2. Drop fully duplicate rows
      3. Drop rows missing critical fields (from_id, to_id, amount)
      4. Filter out non-positive amounts
      5. Cast amount to DoubleType
      6. Parse timestamp column
      7. Add ingestion metadata column
    """
    # 1. Standardise column names
    df = df.toDF(*[c.lower().replace(" ", "_") for c in df.columns])

    # 2. Deduplication
    before = df.count()
    df = df.dropDuplicates()
    logger.info("Removed %d duplicate rows (%d → %d)", before - df.count(), before, df.count())

    # 3. Drop rows missing critical fields
    critical_cols = [
        col for col in [
            _resolve_column(df, ["from_id", "from_account", "fromid"]),
            _resolve_column(df, ["to_id",   "to_account",   "toid"]),
            _resolve_column(df, ["amount",  "amount_paid",  "usd_amount"]),
        ]
        if col is not None
    ]
    if critical_cols:
        df = df.dropna(subset=critical_cols)

    # 4. Filter non-positive amounts
    amount_col = _resolve_column(df, ["amount", "amount_paid", "usd_amount"])
    if amount_col:
        df = df.filter(F.col(amount_col) > 0)
        df = df.withColumn(amount_col, F.col(amount_col).cast(DoubleType()))

    # 5. Parse timestamp
    ts_col = _resolve_column(df, ["timestamp", "date_time", "transaction_date"])
    if ts_col:
        df = df.withColumn(ts_col, F.to_timestamp(F.col(ts_col)))

    # 6. Ingestion metadata
    df = df.withColumn("_ingested_at", F.current_timestamp())

    logger.info("Rows after cleaning: %d", df.count())
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Write
# ─────────────────────────────────────────────────────────────────────────────
def write_silver(
    df: "pyspark.sql.DataFrame",
    write_mode: str = "append",
    gcs_mode: str = "append",
) -> None:
    """Write cleaned data to BigQuery Silver table and GCS Silver bucket.

    BigQuery table is partitioned by DAY on the ``timestamp`` column and
    clustered by ``from_id, to_id`` to optimise time-range scans and
    account-level lookups — critical for AML query patterns.
    """
    row_count = df.count()
    logger.info("Writing %d rows to BigQuery table: %s (mode=%s)", row_count, BQ_TABLE, write_mode)

    ts_col = _resolve_column(df, ["timestamp", "date_time", "transaction_date"])
    from_col = _resolve_column(df, ["from_id", "from_account", "fromid"])
    to_col = _resolve_column(df, ["to_id", "to_account", "toid"])

    writer = (
        df.write.format("bigquery")
        .option("table", BQ_TABLE)
        .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
        .option("createDisposition", "CREATE_IF_NEEDED")
        .option("writeDisposition", f"WRITE_{write_mode.upper()}")
    )

    if ts_col:
        writer = (
            writer
            .option("partitionField", ts_col)
            .option("partitionType", "DAY")
        )

    cluster_cols = [c for c in [from_col, to_col] if c]
    if cluster_cols:
        writer = writer.option("clusteredFields", ",".join(cluster_cols))

    writer.save()
    logger.info("BigQuery write complete (partitioned=%s, clustered=%s).", ts_col, cluster_cols)

    logger.info("Writing to GCS Silver path: %s (mode=%s)", SILVER_GCS_PATH, gcs_mode)
    df.write.mode(gcs_mode).parquet(SILVER_GCS_PATH)
    logger.info("GCS Silver write complete.")
    return row_count


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="AML Silver Layer PySpark job")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--full-refresh",
        action="store_true",
        help="Process all Bronze files (WRITE_TRUNCATE). Default: incremental.",
    )
    mode.add_argument(
        "--partition-date",
        type=str,
        help="Single date partition to process (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--partitions",
        type=str,
        default=None,
        help="Comma-separated list of date partitions to process (YYYY-MM-DD,...). "
             "Used together with --partition-date or alone.",
    )
    parser.add_argument(
        "--skip-manifest",
        action="store_true",
        help="Skip the BigQuery manifest check (useful for first-time setup).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    if args.full_refresh:
        # ── Full-refresh mode ──────────────────────────────────────────────────
        logger.info("Running in FULL-REFRESH mode.")
        df_bronze = read_bronze_full(spark)
        df_silver = clean_dataframe(df_bronze)
        write_silver(df_silver, write_mode="truncate", gcs_mode="overwrite")

    else:
        # ── Incremental mode ───────────────────────────────────────────────────
        # Collect requested partitions
        partitions: List[str] = []
        if args.partition_date:
            partitions.append(args.partition_date)
        if args.partitions:
            partitions.extend(p.strip() for p in args.partitions.split(",") if p.strip())

        if not partitions:
            # Default: process yesterday's partition
            yesterday = (date.today() - timedelta(days=1)).isoformat()
            partitions = [yesterday]
            logger.info("No partitions specified; defaulting to yesterday: %s", yesterday)

        # Skip already-processed partitions
        if not args.skip_manifest:
            processed = get_processed_partitions(spark)
            new_partitions = [p for p in partitions if p not in processed]
            if not new_partitions:
                logger.info("All requested partitions already processed: %s. Exiting.", partitions)
                spark.stop()
                return
            logger.info("New partitions to process: %s (skipping: %s)",
                        new_partitions, set(partitions) - set(new_partitions))
            partitions = new_partitions

        df_bronze = read_bronze_partitions(spark, partitions)
        df_silver = clean_dataframe(df_bronze)
        # row_count is the total across all partitions in this batch.
        # It is stored in the manifest as a batch total — each partition entry
        # records the same number since they were processed together.
        row_count = write_silver(df_silver, write_mode="append", gcs_mode="append")

        # Record successful partitions in manifest
        if not args.skip_manifest:
            for p in partitions:
                record_manifest(spark, p, "SUCCESS", row_count)

    spark.stop()
    logger.info("Silver layer job finished successfully.")


if __name__ == "__main__":
    main()
