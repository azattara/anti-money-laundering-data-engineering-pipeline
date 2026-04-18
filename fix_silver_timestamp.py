"""Update the timestamp column in aml_silver.transactions from aml_bronze.transactions."""
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"c:\Users\aleite\Documents\Data Engineering\anti-money-laundering-data-engineering-pipeline\key.json"

from google.cloud import bigquery
client = bigquery.Client(project="anti-ml-data-engineering")

# Step 1: Check current state
print("=== Before update ===")
q1 = """
SELECT 
  COUNT(*) as total,
  COUNTIF(timestamp IS NOT NULL) as has_ts
FROM `anti-ml-data-engineering.aml_silver.transactions`
"""
for row in client.query(q1).result():
    print(f"Silver: total={row.total:,}, has_timestamp={row.has_ts:,}")

# Step 2: Update timestamp from Bronze
print("\n=== Running UPDATE (this may take a few minutes on 430M rows) ===")
update_sql = """
UPDATE `anti-ml-data-engineering.aml_silver.transactions` s
SET s.timestamp = PARSE_TIMESTAMP('%Y/%m/%d %H:%M', b.timestamp)
FROM `anti-ml-data-engineering.aml_bronze.transactions` b
WHERE s.account2 = b.from_id
  AND s.account4 = b.to_id
  AND s.amount_received = b.amount_received
  AND s._ingested_at = s._ingested_at  -- ensure row match
  AND b.timestamp IS NOT NULL
  AND b.timestamp != ''
"""
job = client.query(update_sql)
result = job.result()
print(f"Rows affected: {job.num_dml_affected_rows:,}")
print(f"Bytes processed: {job.total_bytes_processed / 1024 / 1024 / 1024:.2f} GB")

# Step 3: Verify
print("\n=== After update ===")
for row in client.query(q1).result():
    print(f"Silver: total={row.total:,}, has_timestamp={row.has_ts:,}")

# Step 4: Sample
print("\n=== Sample timestamps ===")
q3 = """
SELECT timestamp, account2, amount_received
FROM `anti-ml-data-engineering.aml_silver.transactions`
WHERE timestamp IS NOT NULL
LIMIT 5
"""
for row in client.query(q3).result():
    print(f"  {row.timestamp}  |  {row.account2}  |  {row.amount_received}")
