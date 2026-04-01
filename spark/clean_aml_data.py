"""
spark/clean_aml_data.py

PySpark job — Silver Layer

Reads the raw IBM AML CSV files from the Bronze GCS bucket,
applies cleaning and validation, then writes the results to BigQuery
(aml_silver dataset) and optionally to the Silver GCS bucket.

Connector: Spark BigQuery connector (spark-bigquery-with-dependencies)
  - Pass the jar via --jars or spark.jars.packages configuration.
  - Tested with: com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0

Usage:
  spark-submit \\
    --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0 \\
    spark/clean_aml_data.py
"""

import os
import logging
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, TimestampType

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "anti-ml-data-engineering")
GCS_BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET", "anti-ml-data-engineering-bronze")
GCS_SILVER_BUCKET = os.getenv("GCS_SILVER_BUCKET", "anti-ml-data-engineering-silver")
BQ_SILVER_DATASET = os.getenv("BQ_SILVER_DATASET", "aml_silver")
TEMP_GCS_BUCKET = os.getenv("GCS_ARTIFACTS_BUCKET", "anti-ml-data-engineering-artifacts")

BRONZE_PATH = f"gs://{GCS_BRONZE_BUCKET}/raw/"
SILVER_GCS_PATH = f"gs://{GCS_SILVER_BUCKET}/cleaned/"
BQ_TABLE = f"{GCP_PROJECT_ID}.{BQ_SILVER_DATASET}.transactions"


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("AML-Silver-Layer")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )


def read_bronze(spark: SparkSession) -> "pyspark.sql.DataFrame":
    """Read all CSV files from the Bronze bucket."""
    logger.info("Reading raw data from: %s", BRONZE_PATH)
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(BRONZE_PATH)
    )
    logger.info("Raw row count: %d", df.count())
    return df


def clean_dataframe(df: "pyspark.sql.DataFrame") -> "pyspark.sql.DataFrame":
    """
    Apply Silver-layer cleaning transformations:
      1. Standardise column names (lowercase, replace spaces with underscores)
      2. Drop fully duplicate rows
      3. Drop rows missing critical fields (from_id, to_id, amount)
      4. Cast amount to DoubleType
      5. Parse timestamp column if present
      6. Add ingestion metadata column
    """
    # 1. Standardise column names
    df = df.toDF(*[c.lower().replace(" ", "_") for c in df.columns])

    # 2. Deduplication
    before = df.count()
    df = df.dropDuplicates()
    after = df.count()
    logger.info("Removed %d duplicate rows (%d → %d)", before - after, before, after)

    # 3. Drop rows missing critical fields (handle variations in column naming)
    critical_cols = []
    for candidate in ("from_id", "from_account", "fromid"):
        if candidate in df.columns:
            critical_cols.append(candidate)
            break
    for candidate in ("to_id", "to_account", "toid"):
        if candidate in df.columns:
            critical_cols.append(candidate)
            break
    for candidate in ("amount", "amount_paid", "usd_amount"):
        if candidate in df.columns:
            critical_cols.append(candidate)
            break

    if critical_cols:
        df = df.dropna(subset=critical_cols)
        logger.info("Rows after dropping nulls in %s: %d", critical_cols, df.count())

    # 4. Cast amount column to Double
    amount_col = None
    for candidate in ("amount", "amount_paid", "usd_amount"):
        if candidate in df.columns:
            amount_col = candidate
            break
    if amount_col:
        df = df.withColumn(amount_col, F.col(amount_col).cast(DoubleType()))

    # 5. Parse timestamp column if present
    for ts_candidate in ("timestamp", "date_time", "transaction_date"):
        if ts_candidate in df.columns:
            df = df.withColumn(ts_candidate, F.to_timestamp(F.col(ts_candidate)))
            break

    # 6. Add ingestion metadata
    df = df.withColumn("_ingested_at", F.current_timestamp())

    logger.info("Schema after cleaning:")
    df.printSchema()
    return df


def write_silver(df: "pyspark.sql.DataFrame", spark: SparkSession) -> None:
    """Write cleaned data to BigQuery Silver table and GCS Silver bucket."""
    logger.info("Writing to BigQuery table: %s", BQ_TABLE)
    (
        df.write.format("bigquery")
        .option("table", BQ_TABLE)
        .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
        .option("createDisposition", "CREATE_IF_NEEDED")
        .option("writeDisposition", "WRITE_TRUNCATE")
        .save()
    )
    logger.info("BigQuery write complete.")

    logger.info("Writing to GCS Silver path: %s", SILVER_GCS_PATH)
    df.write.mode("overwrite").parquet(SILVER_GCS_PATH)
    logger.info("GCS Silver write complete.")


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    df_bronze = read_bronze(spark)
    df_silver = clean_dataframe(df_bronze)
    write_silver(df_silver, spark)

    spark.stop()
    logger.info("Silver layer job finished successfully.")


if __name__ == "__main__":
    main()
