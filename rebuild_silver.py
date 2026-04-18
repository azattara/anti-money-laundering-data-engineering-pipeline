"""Rebuild aml_silver.transactions: DROP then CREATE from Bronze with correct timestamps."""
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"c:\Users\aleite\Documents\Data Engineering\anti-money-laundering-data-engineering-pipeline\key.json"

from google.cloud import bigquery
client = bigquery.Client(project="anti-ml-data-engineering")

# Step 1: DROP existing table
print("=== Step 1: Dropping existing aml_silver.transactions ===")
client.query("DROP TABLE IF EXISTS `anti-ml-data-engineering.aml_silver.transactions`").result()
print("Dropped.")

# Step 2: Recreate from Bronze with parsed timestamps
print("\n=== Step 2: Creating aml_silver.transactions from Bronze ===")
rebuild_sql = """
CREATE TABLE `anti-ml-data-engineering.aml_silver.transactions`
PARTITION BY DATE(timestamp)
CLUSTER BY account2, account4
AS
SELECT
  PARSE_TIMESTAMP('%Y/%m/%d %H:%M', timestamp)  AS timestamp,
  from_bank,
  from_id   AS account2,
  to_bank,
  to_id     AS account4,
  amount_received,
  receiving_currency,
  amount_paid,
  payment_currency,
  payment_format,
  is_laundering,
  CURRENT_TIMESTAMP() AS _ingested_at
FROM `anti-ml-data-engineering.aml_bronze.transactions`
WHERE timestamp IS NOT NULL
  AND timestamp != ''
  AND amount_received > 0
  AND from_id IS NOT NULL
  AND to_id IS NOT NULL
"""

print("Running (430M+ rows — may take a few minutes)...")
job = client.query(rebuild_sql)
result = job.result(timeout=900)
print(f"Done! Bytes processed: {job.total_bytes_processed / 1024**3:.2f} GB")

# Verify
print("\n=== Verification ===")
q1 = """
SELECT 
  COUNT(*) as total,
  COUNTIF(timestamp IS NOT NULL) as has_ts,
  MIN(timestamp) as min_ts,
  MAX(timestamp) as max_ts
FROM `anti-ml-data-engineering.aml_silver.transactions`
"""
for row in client.query(q1).result():
    print(f"Total rows:    {row.total:,}")
    print(f"Has timestamp: {row.has_ts:,}")
    print(f"Min timestamp: {row.min_ts}")
    print(f"Max timestamp: {row.max_ts}")

# Sample
print("\n=== Sample rows ===")
q2 = """
SELECT timestamp, account2, account4, amount_received, payment_format
FROM `anti-ml-data-engineering.aml_silver.transactions`
LIMIT 5
"""
for row in client.query(q2).result():
    print(f"  {row.timestamp}  |  {row.account2}  |  {row.account4}  |  {row.amount_received}  |  {row.payment_format}")
