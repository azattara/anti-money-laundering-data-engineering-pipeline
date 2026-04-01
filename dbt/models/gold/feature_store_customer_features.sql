-- models/gold/feature_store_customer_features.sql
-- Feature Store — Final Customer Feature Table (rule-based selection layer)
--
-- This model is the *publishing* layer of the feature store.  It selects only
-- the features that pass the following explainable rules:
--
--   RULE-1 (Null threshold):  Features with an expected null-rate above 30%
--           in cold-start scenarios are excluded.
--           Excluded: days_since_last_tx on the very first observed day
--           (handled via coalesce below, not by dropping the column).
--
--   RULE-2 (Constant / low-variance):  Features that are trivially constant
--           across all customers (e.g. tx_count_1d when all customers have
--           exactly 1 transaction per day) provide no signal.  The 1-day raw
--           amount stats (min/std) tend to be noisy for single-transaction
--           days and are excluded in favour of rolling equivalents.
--
--   RULE-3 (Redundancy): Highly correlated pairs are reduced to one.
--           tx_amount_max_7d and tx_amount_avg_7d are individually kept but
--           their ratio (amount_max_to_avg_ratio_7d) already captures the
--           relationship, so raw 7d max/avg are kept for model transparency
--           while redundant duplicates at other granularities are dropped.
--
--   RULE-4 (Leakage prevention): is_laundering and any target-related columns
--           must never appear in the feature store output.
--
-- To add a new feature:
--   1. Compute it in mart_customer_features_gold.sql.
--   2. Add it to the SELECT below with a comment referencing which rule it passes.
--   3. Document it in dbt/models/schema.yml under feature_store_customer_features.
--
-- To remove a feature:
--   1. Remove it from the SELECT below.
--   2. Mark it as deprecated in schema.yml with the removal date and reason.

{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['customer_id', 'feature_date'],
        schema='aml_gold',
        partition_by={
            'field': 'feature_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['customer_id'],
        on_schema_change='append_new_columns'
    )
}}

select
    -- ── Feature store keys ────────────────────────────────────────────────────
    customer_id,
    feature_date,

    -- ── Recency (RULE-1: null on first day → coalesced to sentinel -1) ───────
    coalesce(days_since_last_tx, -1)    as days_since_last_tx,

    -- ── Frequency ────────────────────────────────────────────────────────────
    tx_count_7d,                        -- PASS: low null-rate, good variance
    tx_count_30d,                       -- PASS
    tx_count_90d,                       -- PASS

    -- ── Monetary ─────────────────────────────────────────────────────────────
    tx_amount_sum_7d,                   -- PASS: high variance, key AML signal
    tx_amount_avg_7d,                   -- PASS
    tx_amount_max_7d,                   -- PASS: extreme values flag structuring
    tx_amount_sum_30d,                  -- PASS
    tx_amount_avg_30d,                  -- PASS
    tx_amount_sum_90d,                  -- PASS
    tx_amount_avg_90d,                  -- PASS

    -- ── Counterparty diversity ────────────────────────────────────────────────
    unique_counterparties_7d,           -- PASS: strong AML signal
    unique_counterparties_30d,          -- PASS
    unique_counterparties_90d,          -- PASS

    -- ── Currency / payment-method diversity ──────────────────────────────────
    cross_currency_tx_count_30d,        -- PASS: money-laundering indicator
    cross_currency_tx_count_90d,        -- PASS
    unique_payment_currencies_30d,      -- PASS
    unique_payment_formats_90d,         -- PASS

    -- ── AML ratios / derived signals ─────────────────────────────────────────
    cross_currency_ratio_30d,           -- PASS: normalised signal
    cross_currency_ratio_90d,           -- PASS
    counterparty_concentration_30d,     -- PASS: layering proxy
    amount_max_to_avg_ratio_7d,         -- PASS: structuring proxy
    velocity_spike_ratio_7d,            -- PASS: rapid velocity indicator

    -- ── Feature store metadata ────────────────────────────────────────────────
    feature_version,
    source_model,
    feature_created_at

from {{ ref('mart_customer_features_gold') }}

{% if is_incremental() %}
-- Align incremental window with upstream model (90-day lookback)
where feature_date >= date_sub(
    (select max(feature_date) from {{ this }}),
    interval 90 day
)
{% endif %}
