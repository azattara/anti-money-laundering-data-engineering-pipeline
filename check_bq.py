"""Check the actual BigQuery schema of aml_silver.transactions and sample values."""
import os, json
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"c:\Users\aleite\Documents\Data Engineering\anti-money-laundering-data-engineering-pipeline\key.json"

from google.cloud import bigquery
client = bigquery.Client(project="anti-ml-data-engineering")

# 1. Schema
table = client.get_table("anti-ml-data-engineering.aml_silver.transactions")
print(f"Table: {table.full_table_id}")
print(f"Rows: {table.num_rows}")
print(f"Size: {table.num_bytes / 1024 / 1024:.1f} MB")
print(f"\nSchema ({len(table.schema)} columns):")
for field in table.schema:
    print(f"  {field.name:30s} {field.field_type:15s} {field.mode}")

# 2. Sample non-null timestamp values
print("\n--- Sample rows (first 5) ---")
query = """
SELECT * 
FROM `anti-ml-data-engineering.aml_silver.transactions`
LIMIT 5
"""
for row in client.query(query).result():
    print(dict(row))

# 3. Check timestamp specifically
print("\n--- Timestamp analysis ---")
query2 = """
SELECT 
  COUNT(*) as total,
  COUNTIF(timestamp IS NOT NULL) as ts_not_null,
  COUNTIF(CAST(timestamp AS STRING) != '') as ts_not_empty,
  MIN(timestamp) as min_ts,
  MAX(timestamp) as max_ts
FROM `anti-ml-data-engineering.aml_silver.transactions`
"""
for row in client.query(query2).result():
    print(dict(row))
