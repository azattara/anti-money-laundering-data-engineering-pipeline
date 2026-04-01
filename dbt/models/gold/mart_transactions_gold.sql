-- models/gold/mart_transactions_gold.sql
-- Gold layer: aggregated transaction summary per sender-receiver pair.
-- DEPRECATED: kept for backwards compatibility.
-- The canonical feature store is mart_customer_features_gold →
-- feature_store_customer_features (grain: customer_id × feature_date,
-- incremental, partitioned by feature_date, clustered by customer_id).

{{ config(materialized='table', schema='aml_gold') }}

select
    from_id,
    to_id,
    count(*)                          as transaction_count,
    sum(amount)                       as total_amount,
    avg(amount)                       as avg_amount,
    min(amount)                       as min_amount,
    max(amount)                       as max_amount,
    min(timestamp)                    as first_transaction_at,
    max(timestamp)                    as last_transaction_at
from {{ ref('stg_transactions_silver') }}
where row_num = 1
group by from_id, to_id
