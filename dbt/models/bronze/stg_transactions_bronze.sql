-- models/bronze/stg_transactions_bronze.sql
-- Bronze layer: expose raw transactions from BigQuery (aml_bronze dataset)
-- This is a thin view over the raw ingested data — no transformations applied.

{{ config(materialized='view', schema='aml_bronze') }}

select *
from {{ source('aml_bronze', 'transactions') }}
