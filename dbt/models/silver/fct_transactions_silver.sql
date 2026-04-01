-- models/silver/fct_transactions_silver.sql
-- Silver layer: clean, typed, deduplicated transaction fact table.
-- This model is the canonical source for all Gold/feature-store models.
-- Grain: one row per unique transaction (from_id, to_id, timestamp, amount).

{{ config(
    materialized='table',
    schema='aml_silver'
) }}

with source as (
    select * from {{ source('aml_silver', 'transactions') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by from_id, to_id, amount, timestamp
            order by _ingested_at desc
        ) as _row_num
    from source
    where
        amount > 0
        and from_id is not null
        and to_id   is not null
        and timestamp is not null
),

cleaned as (
    select
        -- Primary identifiers
        cast(from_id   as string) as customer_id,
        cast(to_id     as string) as counterparty_id,

        -- Transaction facts
        cast(amount    as float64)                  as amount,
        cast(timestamp as timestamp)                as event_time,
        date(cast(timestamp as timestamp))          as feature_date,

        -- Optional columns — kept with safe defaults so the model works
        -- whether or not the raw source contains them.
        if(safe_cast(is_laundering as int64) is not null,
           safe_cast(is_laundering as int64), null)  as is_laundering,

        coalesce(nullif(trim(payment_format), ''), 'UNKNOWN')
                                                     as payment_format,

        coalesce(nullif(trim(payment_currency), ''), 'UNKNOWN')
                                                     as payment_currency,

        coalesce(nullif(trim(receiving_currency), ''), 'UNKNOWN')
                                                     as receiving_currency,

        -- Metadata
        _ingested_at

    from deduplicated
    where _row_num = 1
)

select * from cleaned
