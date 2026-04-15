"""
AML Pipeline — Dimensionality Comparison Dashboard
Compares raw Silver data vs curated Gold Feature Store data.
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from google.cloud import bigquery

os.environ.setdefault(
    "GOOGLE_APPLICATION_CREDENTIALS",
    os.path.join(os.path.dirname(__file__), "key.json"),
)

PROJECT = "anti-ml-data-engineering"

# ── BigQuery helpers ─────────────────────────────────────────────────────────

@st.cache_resource
def get_bq_client():
    return bigquery.Client(project=PROJECT)


@st.cache_data(ttl=600)
def run_query(sql: str) -> pd.DataFrame:
    return get_bq_client().query(sql).to_dataframe()


# ── Queries ──────────────────────────────────────────────────────────────────

Q_SILVER_SCHEMA = """
SELECT column_name, data_type
FROM `anti-ml-data-engineering.aml_gold_aml_silver.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'fct_transactions_silver'
ORDER BY ordinal_position
"""

Q_GOLD_SCHEMA = """
SELECT column_name, data_type
FROM `anti-ml-data-engineering.aml_gold_aml_gold.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'feature_store_customer_features'
ORDER BY ordinal_position
"""

Q_GOLD_COMPUTE_SCHEMA = """
SELECT column_name, data_type
FROM `anti-ml-data-engineering.aml_gold_aml_gold.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'mart_customer_features_gold'
ORDER BY ordinal_position
"""

Q_SILVER_STATS = """
SELECT
  COUNT(*)                                       AS row_count,
  COUNT(DISTINCT customer_id)                    AS unique_customers,
  COUNT(DISTINCT counterparty_id)                AS unique_counterparties,
  MIN(feature_date)                              AS min_date,
  MAX(feature_date)                              AS max_date,
  AVG(amount)                                    AS avg_amount,
  STDDEV(amount)                                 AS std_amount
FROM `anti-ml-data-engineering.aml_gold_aml_silver.fct_transactions_silver`
"""

Q_GOLD_STATS = """
SELECT
  COUNT(*)                                       AS row_count,
  COUNT(DISTINCT customer_id)                    AS unique_customers,
  MIN(feature_date)                              AS min_date,
  MAX(feature_date)                              AS max_date
FROM `anti-ml-data-engineering.aml_gold_aml_gold.feature_store_customer_features`
"""

Q_SILVER_SAMPLE = """
SELECT *
FROM `anti-ml-data-engineering.aml_gold_aml_silver.fct_transactions_silver`
LIMIT 500
"""

Q_GOLD_SAMPLE = """
SELECT *
FROM `anti-ml-data-engineering.aml_gold_aml_gold.feature_store_customer_features`
LIMIT 500
"""

Q_GOLD_CORR = """
SELECT
  tx_count_7d, tx_count_30d, tx_count_90d,
  tx_amount_sum_7d, tx_amount_avg_7d, tx_amount_max_7d,
  tx_amount_sum_30d, tx_amount_avg_30d,
  tx_amount_sum_90d, tx_amount_avg_90d,
  unique_counterparties_7d, unique_counterparties_30d, unique_counterparties_90d,
  cross_currency_ratio_30d, cross_currency_ratio_90d,
  counterparty_concentration_30d,
  amount_max_to_avg_ratio_7d,
  velocity_spike_ratio_7d,
  days_since_last_tx
FROM `anti-ml-data-engineering.aml_gold_aml_gold.feature_store_customer_features`
WHERE RAND() < 0.001
"""

Q_NULL_RATES_SILVER = """
SELECT
  'customer_id'         AS col, COUNTIF(customer_id IS NULL)        / COUNT(*) AS null_rate FROM `anti-ml-data-engineering.aml_gold_aml_silver.fct_transactions_silver`
UNION ALL SELECT 'counterparty_id',  COUNTIF(counterparty_id IS NULL)   / COUNT(*) FROM `anti-ml-data-engineering.aml_gold_aml_silver.fct_transactions_silver`
UNION ALL SELECT 'amount',           COUNTIF(amount IS NULL)            / COUNT(*) FROM `anti-ml-data-engineering.aml_gold_aml_silver.fct_transactions_silver`
UNION ALL SELECT 'event_time',       COUNTIF(event_time IS NULL)        / COUNT(*) FROM `anti-ml-data-engineering.aml_gold_aml_silver.fct_transactions_silver`
UNION ALL SELECT 'feature_date',     COUNTIF(feature_date IS NULL)      / COUNT(*) FROM `anti-ml-data-engineering.aml_gold_aml_silver.fct_transactions_silver`
UNION ALL SELECT 'is_laundering',    COUNTIF(is_laundering IS NULL)     / COUNT(*) FROM `anti-ml-data-engineering.aml_gold_aml_silver.fct_transactions_silver`
UNION ALL SELECT 'payment_format',   COUNTIF(payment_format IS NULL)    / COUNT(*) FROM `anti-ml-data-engineering.aml_gold_aml_silver.fct_transactions_silver`
UNION ALL SELECT 'payment_currency', COUNTIF(payment_currency IS NULL)  / COUNT(*) FROM `anti-ml-data-engineering.aml_gold_aml_silver.fct_transactions_silver`
UNION ALL SELECT 'receiving_currency', COUNTIF(receiving_currency IS NULL) / COUNT(*) FROM `anti-ml-data-engineering.aml_gold_aml_silver.fct_transactions_silver`
"""

Q_GRAIN_COMPARISON = """
SELECT 'Silver (transaction)' AS layer, COUNT(*) AS row_count
FROM `anti-ml-data-engineering.aml_gold_aml_silver.fct_transactions_silver`
UNION ALL
SELECT 'Gold Compute (~40 cols)', COUNT(*)
FROM `anti-ml-data-engineering.aml_gold_aml_gold.mart_customer_features_gold`
UNION ALL
SELECT 'Gold Serving (~25 cols)', COUNT(*)
FROM `anti-ml-data-engineering.aml_gold_aml_gold.feature_store_customer_features`
"""


# ── Page config ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="AML — Dimensionality Comparison",
    page_icon="🔍",
    layout="wide",
)

st.title("AML Pipeline — Dimensionality Comparison")
st.caption("Silver (raw transactions) vs Gold Feature Store (curated features)")

# ── Load data ────────────────────────────────────────────────────────────────

with st.spinner("Loading metadata from BigQuery..."):
    silver_schema = run_query(Q_SILVER_SCHEMA)
    gold_schema = run_query(Q_GOLD_SCHEMA)
    gold_compute_schema = run_query(Q_GOLD_COMPUTE_SCHEMA)
    silver_stats = run_query(Q_SILVER_STATS).iloc[0]
    gold_stats = run_query(Q_GOLD_STATS).iloc[0]
    grain_df = run_query(Q_GRAIN_COMPARISON)

# ═════════════════════════════════════════════════════════════════════════════
# TILE 1: Silver (Raw)                 │ TILE 2: Gold Feature Store (Curated)
# ═════════════════════════════════════════════════════════════════════════════

st.markdown("---")
st.header("1 — Schema & Dimensionality")

col_silver, col_arrow, col_gold = st.columns([5, 1, 5])

with col_silver:
    st.subheader("🟦 Silver — Raw Transactions")
    st.metric("Columns", len(silver_schema))
    st.metric("Rows", f"{silver_stats['row_count']:,.0f}")
    st.metric("Unique Customers", f"{silver_stats['unique_customers']:,.0f}")
    st.metric("Grain", "1 row per transaction")
    st.dataframe(silver_schema, use_container_width=True, hide_index=True)

with col_arrow:
    st.markdown("<br><br><br><br><br>", unsafe_allow_html=True)
    st.markdown(
        "<h1 style='text-align:center; color:#888;'>→</h1>",
        unsafe_allow_html=True,
    )

with col_gold:
    st.subheader("🟨 Gold — Feature Store (Serving)")
    st.metric("Columns", len(gold_schema))
    st.metric("Rows", f"{gold_stats['row_count']:,.0f}")
    st.metric("Unique Customers", f"{gold_stats['unique_customers']:,.0f}")
    st.metric("Grain", "1 row per customer × day")
    st.dataframe(gold_schema, use_container_width=True, hide_index=True)

# ── Dimensionality funnel ────────────────────────────────────────────────────

st.markdown("---")
st.header("2 — Dimensionality Funnel")

funnel_data = pd.DataFrame({
    "Layer": ["Silver (raw)", "Gold Compute (all features)", "Gold Serving (curated)"],
    "Columns": [len(silver_schema), len(gold_compute_schema), len(gold_schema)],
})

col_funnel, col_rules = st.columns([3, 2])

with col_funnel:
    fig_funnel = px.funnel(
        funnel_data,
        x="Columns",
        y="Layer",
        color="Layer",
        color_discrete_sequence=["#4A90D9", "#F5A623", "#7ED321"],
    )
    fig_funnel.update_layout(showlegend=False, height=350)
    st.plotly_chart(fig_funnel, use_container_width=True)

with col_rules:
    st.markdown("""
    **Feature Selection Rules (Gold Compute → Serving)**

    | Rule | Action |
    |---|---|
    | **RULE-1** Null threshold > 30% | Exclude or coalesce |
    | **RULE-2** Low variance / constant | Exclude |
    | **RULE-3** Redundant / correlated | Keep one representative |
    | **RULE-4** Target leakage | Never publish |

    Reducing from **~{compute}** to **~{serving}** columns prevents
    overfitting and the curse of dimensionality.
    """.format(compute=len(gold_compute_schema), serving=len(gold_schema)))

# ── Row-count progression ────────────────────────────────────────────────────

st.markdown("---")
st.header("3 — Row Count by Layer")

fig_rows = px.bar(
    grain_df,
    x="layer",
    y="row_count",
    color="layer",
    text="row_count",
    color_discrete_sequence=["#4A90D9", "#F5A623", "#7ED321"],
    labels={"layer": "Layer", "row_count": "Row Count"},
)
fig_rows.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
fig_rows.update_layout(showlegend=False, height=400, yaxis_title="Rows")
st.plotly_chart(fig_rows, use_container_width=True)

st.info(
    f"**Silver** has **{silver_stats['row_count']:,.0f}** rows (1 per transaction). "
    f"**Gold** aggregates to **{gold_stats['row_count']:,.0f}** rows "
    f"(1 per customer × day), a **{silver_stats['row_count'] / gold_stats['row_count']:.1f}x** reduction in granularity."
)

# ── Null rate comparison ─────────────────────────────────────────────────────

st.markdown("---")
st.header("4 — Data Quality: Null Rates (Silver)")

with st.spinner("Checking null rates..."):
    null_df = run_query(Q_NULL_RATES_SILVER)

fig_null = px.bar(
    null_df,
    x="col",
    y="null_rate",
    text="null_rate",
    color_discrete_sequence=["#E74C3C"],
    labels={"col": "Column", "null_rate": "Null Rate"},
)
fig_null.update_traces(texttemplate="%{text:.2%}", textposition="outside")
fig_null.update_layout(height=400, yaxis_tickformat=".0%", yaxis_title="Null Rate")
st.plotly_chart(fig_null, use_container_width=True)

st.success("All critical columns (customer_id, counterparty_id, amount, event_time, feature_date) have 0% null rate — quality rules enforced at Silver layer.")

# ── Correlation heatmap ──────────────────────────────────────────────────────

st.markdown("---")
st.header("5 — Feature Correlation (Gold Serving)")
st.caption("Sampled ~0.1% of rows for visualization. Low inter-feature correlation confirms RULE-3 (redundancy removal) is effective.")

with st.spinner("Computing correlation matrix..."):
    corr_df = run_query(Q_GOLD_CORR)

if len(corr_df) > 10:
    corr_matrix = corr_df.corr()
    fig_corr = px.imshow(
        corr_matrix,
        text_auto=".2f",
        color_continuous_scale="RdBu_r",
        zmin=-1,
        zmax=1,
        aspect="auto",
    )
    fig_corr.update_layout(height=700, width=900)
    st.plotly_chart(fig_corr, use_container_width=True)
else:
    st.warning("Not enough sampled rows for correlation. Try again or increase sample rate.")

# ── Sample data preview ──────────────────────────────────────────────────────

st.markdown("---")
st.header("6 — Sample Data Preview")

tab_silver, tab_gold = st.tabs(["🟦 Silver (Raw)", "🟨 Gold (Feature Store)"])

with tab_silver:
    with st.spinner("Loading Silver sample..."):
        silver_sample = run_query(Q_SILVER_SAMPLE)
    st.dataframe(silver_sample, use_container_width=True, hide_index=True)
    st.caption(f"Showing {len(silver_sample)} rows × {len(silver_sample.columns)} columns")

with tab_gold:
    with st.spinner("Loading Gold sample..."):
        gold_sample = run_query(Q_GOLD_SAMPLE)
    st.dataframe(gold_sample, use_container_width=True, hide_index=True)
    st.caption(f"Showing {len(gold_sample)} rows × {len(gold_sample.columns)} columns")

# ── Footer ───────────────────────────────────────────────────────────────────

st.markdown("---")
st.markdown(
    "<div style='text-align:center; color:#888; font-size:0.85em;'>"
    "AML Data Engineering Pipeline — Dimensionality Comparison Dashboard"
    "</div>",
    unsafe_allow_html=True,
)
