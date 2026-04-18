import csv
with open('aml_silver_sample.csv') as f:
    r = csv.DictReader(f)
    rows = list(r)
    print(f"total rows: {len(rows)}")
    print(f"columns: {rows[0].keys()}")
    ts_vals = set(row['timestamp'] for row in rows[:100])
    print(f"timestamp unique values (first 100 rows): {ts_vals}")
    non_empty = [row['timestamp'] for row in rows if row['timestamp'].strip()]
    print(f"non-empty timestamp count: {len(non_empty)}")
    if non_empty:
        print(f"sample: {non_empty[:5]}")
