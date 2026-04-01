-- models/silver/stg_transactions_silver.sql
-- Silver layer: cleaned and validated transactions (output of PySpark job)
-- Adds a row_number for deduplication safety and filters out invalid amounts.

{{ config(materialized='table', schema='aml_silver') }}

select
    *,
    row_number() over (partition by from_id, to_id, amount, timestamp order by _ingested_at desc) as row_num
from {{ source('aml_silver', 'transactions') }}
where amount > 0
