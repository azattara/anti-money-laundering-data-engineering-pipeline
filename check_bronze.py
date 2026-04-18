"""Check the Bronze table schema and a sample timestamp value."""
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"c:\Users\aleite\Documents\Data Engineering\anti-money-laundering-data-engineering-pipeline\key.json"

from google.cloud import bigquery
client = bigquery.Client(project="anti-ml-data-engineering")

# 1. Bronze schema
table = client.get_table("anti-ml-data-engineering.aml_bronze.transactions")
print(f"Bronze table: {table.full_table_id}")
print(f"Rows: {table.num_rows}")
print(f"\nSchema ({len(table.schema)} columns):")
for field in table.schema:
    print(f"  {field.name:30s} {field.field_type:15s} {field.mode}")

# 2. Sample timestamp from Bronze
print("\n--- Sample rows (first 3) ---")
query = """
SELECT *
FROM `anti-ml-data-engineering.aml_bronze.transactions`
LIMIT 3
"""
for row in client.query(query).result():
    d = dict(row)
    for k, v in d.items():
        print(f"  {k}: [{v}]")
    print()
