{{ config(materialized='view', schema='aml_bronze') }}

select *
from {{ source('aml_bronze', 'transactions') }}