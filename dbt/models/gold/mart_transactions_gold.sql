-- models/gold/mart_transactions_gold.sql
-- Gold layer: aggregated transaction summary per sender-receiver pair.
-- Grain: (customer_id, counterparty_id) — one row per unique pair.
--
-- Incremental logic:
--   On an incremental run → reprocess pairs that had new activity in the
--   last 90 days (aligned with the FATF/FinCEN lookback window).
--   Uses delete+insert strategy scoped to affected pairs.

{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['customer_id', 'counterparty_id'],
        schema='aml_gold',
        partition_by={
            'field': 'last_transaction_at',
            'data_type': 'timestamp',
            'granularity': 'day'
        },
        cluster_by=['customer_id']
    )
}}

with txn as (
    select
        customer_id,
        counterparty_id,
        amount,
        event_time
    from {{ ref('fct_transactions_silver') }}

    {% if is_incremental() %}
    where feature_date >= date_sub(
        (select max(date(last_transaction_at)) from {{ this }}),
        interval 90 day
    )
    {% endif %}
)

select
    customer_id,
    counterparty_id,
    count(*)                          as transaction_count,
    sum(amount)                       as total_amount,
    avg(amount)                       as avg_amount,
    min(amount)                       as min_amount,
    max(amount)                       as max_amount,
    min(event_time)                   as first_transaction_at,
    max(event_time)                   as last_transaction_at
from txn
group by customer_id, counterparty_id
