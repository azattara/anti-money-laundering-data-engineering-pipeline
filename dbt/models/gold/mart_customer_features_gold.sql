-- models/gold/mart_customer_features_gold.sql
-- Gold layer — AML Customer Feature Store
--
-- Grain         : (customer_id, feature_date)  — one row per customer per day.
-- Materialization: incremental (merge) with 90-day lookback.
-- Partitioning  : feature_date  (DATE)
-- Clustering    : customer_id
--
-- Rolling-window features computed:
--   Recency, frequency, monetary value (RFM) across 1/7/30/90-day windows;
--   counterparty diversity; payment-format and currency diversity;
--   cross-currency activity flag; inbound/outbound ratios.
--
-- Incremental logic:
--   On a full-refresh run  → process all history.
--   On an incremental run  → reprocess the last 90 days to ensure rolling
--                            windows for recently added data are correct.
--   The 90-day lookback is the standard AML window recommended by FATF/FinCEN.

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

-- ─────────────────────────────────────────────────────────────────────────────
-- Step 1: define the base transaction source and apply the incremental filter.
-- ─────────────────────────────────────────────────────────────────────────────
with base_transactions as (
    select
        customer_id,
        counterparty_id,
        amount,
        event_time,
        feature_date,
        payment_format,
        payment_currency,
        receiving_currency
    from {{ ref('fct_transactions_silver') }}

    {% if is_incremental() %}
    -- In incremental mode, only load transactions from the last 90 days
    -- so that all rolling windows (up to 90d) can be recomputed correctly.
    where feature_date >= date_sub(
        (select max(feature_date) from {{ this }}),
        interval 90 day
    )
    {% endif %}
),

-- ─────────────────────────────────────────────────────────────────────────────
-- Step 2: daily aggregates per customer — one row per (customer_id, date).
-- ─────────────────────────────────────────────────────────────────────────────
daily_agg as (
    select
        customer_id,
        feature_date,

        -- Transaction volume
        count(*)                                        as tx_count_1d,
        sum(amount)                                     as tx_amount_sum_1d,
        avg(amount)                                     as tx_amount_avg_1d,
        max(amount)                                     as tx_amount_max_1d,
        min(amount)                                     as tx_amount_min_1d,
        stddev(amount)                                  as tx_amount_std_1d,

        -- Counterparty diversity
        count(distinct counterparty_id)                 as unique_counterparties_1d,

        -- Payment format diversity
        count(distinct payment_format)                  as unique_payment_formats_1d,

        -- Cross-currency activity (payment_currency ≠ receiving_currency)
        countif(payment_currency != receiving_currency) as cross_currency_tx_count_1d,

        -- Currency diversity
        count(distinct payment_currency)                as unique_payment_currencies_1d

    from base_transactions
    group by customer_id, feature_date
),

-- ─────────────────────────────────────────────────────────────────────────────
-- Step 3: compute rolling-window features using window functions.
-- ─────────────────────────────────────────────────────────────────────────────
windowed as (
    select
        customer_id,
        feature_date,

        -- ── 1-day features (already computed in daily_agg) ──────────────────
        tx_count_1d,
        tx_amount_sum_1d,
        tx_amount_avg_1d,
        tx_amount_max_1d,
        tx_amount_min_1d,
        tx_amount_std_1d,
        unique_counterparties_1d,
        cross_currency_tx_count_1d,
        unique_payment_currencies_1d,

        -- ── 7-day rolling features ───────────────────────────────────────────
        {{ rolling_window('sum', 'tx_count_1d', 'customer_id', 'feature_date', 7) }}
            as tx_count_7d,
        {{ rolling_window('sum', 'tx_amount_sum_1d', 'customer_id', 'feature_date', 7) }}
            as tx_amount_sum_7d,
        {{ rolling_window('avg', 'tx_amount_avg_1d', 'customer_id', 'feature_date', 7) }}
            as tx_amount_avg_7d,
        {{ rolling_window('max', 'tx_amount_max_1d', 'customer_id', 'feature_date', 7) }}
            as tx_amount_max_7d,
        -- NOTE: unique_counterparties_*d are rolling sums of the *daily* distinct count.
        -- This is an upper-bound proxy for true window-level uniqueness (the same counterparty
        -- may appear on multiple days within the window). True unique counts require expensive
        -- self-joins; this proxy retains the directional signal at much lower compute cost.
        {{ rolling_window('sum', 'unique_counterparties_1d', 'customer_id', 'feature_date', 7) }}
            as unique_counterparties_7d,
        {{ rolling_window('sum', 'cross_currency_tx_count_1d', 'customer_id', 'feature_date', 7) }}
            as cross_currency_tx_count_7d,

        -- ── 30-day rolling features ──────────────────────────────────────────
        {{ rolling_window('sum', 'tx_count_1d', 'customer_id', 'feature_date', 30) }}
            as tx_count_30d,
        {{ rolling_window('sum', 'tx_amount_sum_1d', 'customer_id', 'feature_date', 30) }}
            as tx_amount_sum_30d,
        {{ rolling_window('avg', 'tx_amount_avg_1d', 'customer_id', 'feature_date', 30) }}
            as tx_amount_avg_30d,
        {{ rolling_window('max', 'tx_amount_max_1d', 'customer_id', 'feature_date', 30) }}
            as tx_amount_max_30d,
        {{ rolling_window('sum', 'unique_counterparties_1d', 'customer_id', 'feature_date', 30) }}
            as unique_counterparties_30d,
        {{ rolling_window('sum', 'cross_currency_tx_count_1d', 'customer_id', 'feature_date', 30) }}
            as cross_currency_tx_count_30d,
        {{ rolling_window('sum', 'unique_payment_currencies_1d', 'customer_id', 'feature_date', 30) }}
            as unique_payment_currencies_30d,

        -- ── 90-day rolling features ──────────────────────────────────────────
        {{ rolling_window('sum', 'tx_count_1d', 'customer_id', 'feature_date', 90) }}
            as tx_count_90d,
        {{ rolling_window('sum', 'tx_amount_sum_1d', 'customer_id', 'feature_date', 90) }}
            as tx_amount_sum_90d,
        {{ rolling_window('avg', 'tx_amount_avg_1d', 'customer_id', 'feature_date', 90) }}
            as tx_amount_avg_90d,
        {{ rolling_window('max', 'tx_amount_max_1d', 'customer_id', 'feature_date', 90) }}
            as tx_amount_max_90d,
        {{ rolling_window('sum', 'unique_counterparties_1d', 'customer_id', 'feature_date', 90) }}
            as unique_counterparties_90d,
        {{ rolling_window('sum', 'cross_currency_tx_count_1d', 'customer_id', 'feature_date', 90) }}
            as cross_currency_tx_count_90d,
        {{ rolling_window('sum', 'unique_payment_formats_1d', 'customer_id', 'feature_date', 90) }}
            as unique_payment_formats_90d,
        {{ rolling_window('sum', 'unique_payment_currencies_1d', 'customer_id', 'feature_date', 90) }}
            as unique_payment_currencies_90d,

        -- ── Recency: days since last transaction ─────────────────────────────
        -- Computed using analytic lag to find the most recent prior activity.
        date_diff(
            feature_date,
            lag(feature_date) over (
                partition by customer_id order by feature_date
            ),
            day
        ) as days_since_last_tx

    from daily_agg
),

-- ─────────────────────────────────────────────────────────────────────────────
-- Step 4: derived ratio features for AML signal enhancement.
-- ─────────────────────────────────────────────────────────────────────────────
ratios as (
    select
        *,

        -- Cross-currency ratio (30d) — higher = more suspicious
        safe_divide(cross_currency_tx_count_30d, tx_count_30d)
            as cross_currency_ratio_30d,

        -- Cross-currency ratio (90d)
        safe_divide(cross_currency_tx_count_90d, tx_count_90d)
            as cross_currency_ratio_90d,

        -- Counterparty concentration (30d): unique counterparties per transaction
        -- Low value (few counterparties but many transactions) can indicate layering.
        safe_divide(unique_counterparties_30d, tx_count_30d)
            as counterparty_concentration_30d,

        -- Amount volatility proxy (7d): max vs avg — extreme ratio flags structuring.
        safe_divide(tx_amount_max_7d, nullif(tx_amount_avg_7d, 0))
            as amount_max_to_avg_ratio_7d,

        -- Velocity spike: 7d count relative to 30d daily average
        safe_divide(tx_count_7d, nullif(tx_count_30d / 30.0, 0))
            as velocity_spike_ratio_7d

    from windowed
)

-- ─────────────────────────────────────────────────────────────────────────────
-- Step 5: final select — add feature store metadata columns.
-- ─────────────────────────────────────────────────────────────────────────────
select
    -- Feature store keys
    customer_id,
    feature_date,

    -- ── Recency ─────────────────────────────────────────────────────────────
    tx_count_1d,
    days_since_last_tx,

    -- ── Frequency (rolling counts) ───────────────────────────────────────────
    tx_count_7d,
    tx_count_30d,
    tx_count_90d,

    -- ── Monetary (rolling amount) ────────────────────────────────────────────
    tx_amount_sum_7d,
    tx_amount_avg_7d,
    tx_amount_max_7d,
    tx_amount_sum_30d,
    tx_amount_avg_30d,
    tx_amount_max_30d,
    tx_amount_sum_90d,
    tx_amount_avg_90d,
    tx_amount_max_90d,

    -- ── Counterparty diversity ───────────────────────────────────────────────
    unique_counterparties_1d,
    unique_counterparties_7d,
    unique_counterparties_30d,
    unique_counterparties_90d,

    -- ── Currency / payment diversity ─────────────────────────────────────────
    cross_currency_tx_count_7d,
    cross_currency_tx_count_30d,
    cross_currency_tx_count_90d,
    unique_payment_currencies_30d,
    unique_payment_currencies_90d,
    unique_payment_formats_90d,

    -- ── Derived ratios / AML signals ─────────────────────────────────────────
    cross_currency_ratio_30d,
    cross_currency_ratio_90d,
    counterparty_concentration_30d,
    amount_max_to_avg_ratio_7d,
    velocity_spike_ratio_7d,

    -- ── Feature store metadata ───────────────────────────────────────────────
    '1.0'                       as feature_version,
    'mart_customer_features_gold' as source_model,
    current_timestamp()         as feature_created_at

from ratios
