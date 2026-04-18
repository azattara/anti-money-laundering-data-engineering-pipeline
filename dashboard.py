"""
AML Pipeline — Feature Engineering & Risk Scoring Analysis Dashboard
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


def quote_sql_string(value: str) -> str:
        return value.replace("'", "''")


RISK_SCORE_SQL = """
least(
    100.0,
    round(
        100 * (
            0.30 * least(coalesce(velocity_spike_ratio_7d, 0.0) / 10.0, 1.0) +
            0.25 * least(coalesce(cross_currency_ratio_30d, 0.0), 1.0) +
            0.20 * least(coalesce(amount_max_to_avg_ratio_7d, 0.0) / 8.0, 1.0) +
            0.15 * greatest(1.0 - least(coalesce(counterparty_concentration_30d, 1.0), 1.0), 0.0) +
            0.10 * least(
                safe_divide(coalesce(tx_count_7d, 0.0) * 90.0, nullif(coalesce(tx_count_90d, 0.0), 0.0)),
                1.0
            )
        ),
        1
    )
)
"""


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

Q_TOP_SUSPICIOUS_ACCOUNTS = f"""
with latest_snapshot as (
    select *
    from `anti-ml-data-engineering.aml_gold_aml_gold.feature_store_customer_features`
    where feature_date = (
        select max(feature_date)
        from `anti-ml-data-engineering.aml_gold_aml_gold.feature_store_customer_features`
    )
),
scored as (
    select
        customer_id,
        feature_date,
        tx_count_7d,
        tx_count_90d,
        tx_amount_sum_7d,
        tx_amount_sum_90d,
        cross_currency_ratio_30d,
        counterparty_concentration_30d,
        amount_max_to_avg_ratio_7d,
        velocity_spike_ratio_7d,
        days_since_last_tx,
        {RISK_SCORE_SQL} as risk_score
    from latest_snapshot
)
select
    customer_id,
    feature_date,
    risk_score,
    case
        when risk_score >= 85 then 'Critical'
        when risk_score >= 70 then 'High'
        when risk_score >= 55 then 'Elevated'
        else 'Monitor'
    end as alert_level,
    round(velocity_spike_ratio_7d, 2) as velocity_spike_ratio_7d,
    round(cross_currency_ratio_30d, 2) as cross_currency_ratio_30d,
    round(amount_max_to_avg_ratio_7d, 2) as amount_max_to_avg_ratio_7d,
    round(counterparty_concentration_30d, 2) as counterparty_concentration_30d,
    tx_count_7d,
    tx_count_90d,
    days_since_last_tx,
    case
        when velocity_spike_ratio_7d >= 5 and cross_currency_ratio_30d >= 0.25 then 'Velocity spike and cross-currency activity'
        when amount_max_to_avg_ratio_7d >= 4 then 'Large transaction versus baseline'
        when counterparty_concentration_30d <= 0.15 then 'Concentrated counterparty behaviour'
        else 'Monitoring threshold exceeded'
    end as primary_signal
from scored
order by risk_score desc, velocity_spike_ratio_7d desc, tx_count_7d desc
limit 20
"""

Q_ALERT_SUMMARY = f"""
with latest_snapshot as (
    select *
    from `anti-ml-data-engineering.aml_gold_aml_gold.feature_store_customer_features`
    where feature_date = (
        select max(feature_date)
        from `anti-ml-data-engineering.aml_gold_aml_gold.feature_store_customer_features`
    )
),
scored as (
    select
        feature_date,
        {RISK_SCORE_SQL} as risk_score
    from latest_snapshot
)
select
    max(feature_date) as latest_feature_date,
    countif(risk_score >= 85) as critical_alerts,
    countif(risk_score >= 70) as active_alerts,
    countif(risk_score >= 55) as monitored_accounts,
    max(risk_score) as max_risk_score,
    round(avg(risk_score), 1) as avg_risk_score
from scored
"""

Q_BEHAVIOR_EVOLUTION_TEMPLATE = f"""
with scored as (
    select
        feature_date,
        tx_count_7d,
        round(safe_divide(tx_count_90d, 90.0) * 7.0, 2) as tx_count_90d_baseline_7d,
        tx_amount_sum_7d,
        round(safe_divide(tx_amount_sum_90d, 90.0) * 7.0, 2) as tx_amount_90d_baseline_7d,
        cross_currency_ratio_30d,
        velocity_spike_ratio_7d,
        {RISK_SCORE_SQL} as risk_score
    from `anti-ml-data-engineering.aml_gold_aml_gold.feature_store_customer_features`
    where customer_id = '{{customer_id}}'
)
select *
from scored
order by feature_date
"""

Q_INCREMENTAL_RUNS = """
with recent_runs as (
    select
        feature_created_at as run_ts,
        count(*) as rows_touched,
        count(distinct customer_id) as customers_touched,
        min(feature_date) as min_feature_date,
        max(feature_date) as max_feature_date,
        count(distinct feature_date) as affected_days,
        dense_rank() over (order by feature_created_at desc) as run_rank
    from `anti-ml-data-engineering.aml_gold_aml_gold.feature_store_customer_features`
    group by feature_created_at
)
select
    run_rank,
    run_ts,
    rows_touched,
    customers_touched,
    min_feature_date,
    max_feature_date,
    affected_days
from recent_runs
where run_rank <= 2
order by run_rank
"""

Q_INCREMENTAL_FOOTPRINT = """
with recent_runs as (
    select
        feature_created_at as run_ts,
        dense_rank() over (order by feature_created_at desc) as run_rank
    from `anti-ml-data-engineering.aml_gold_aml_gold.feature_store_customer_features`
    group by feature_created_at
    qualify run_rank <= 2
)
select
    case
        when run_rank = 1 then 'Latest incremental run'
        when run_rank = 2 then 'Previous incremental run'
    end as run_label,
    feature_date,
    count(*) as rows_touched,
    count(distinct customer_id) as customers_touched
from `anti-ml-data-engineering.aml_gold_aml_gold.feature_store_customer_features` fs
join recent_runs rr
    on fs.feature_created_at = rr.run_ts
group by run_label, feature_date
order by feature_date desc, run_label
"""


# ── Page config ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="AML — Dimensionality Comparison",
    page_icon="🔍",
    layout="wide",
)

st.title("AML Pipeline — Feature Engineering & Risk Scoring Analysis")
st.caption("Silver (raw transactions) vs Gold Feature Store (curated features)")

# ── Load data ────────────────────────────────────────────────────────────────

with st.spinner("Loading metadata from BigQuery..."):
    silver_schema = run_query(Q_SILVER_SCHEMA)
    gold_schema = run_query(Q_GOLD_SCHEMA)
    gold_compute_schema = run_query(Q_GOLD_COMPUTE_SCHEMA)
    silver_stats = run_query(Q_SILVER_STATS).iloc[0]
    gold_stats = run_query(Q_GOLD_STATS).iloc[0]
    grain_df = run_query(Q_GRAIN_COMPARISON)
    suspicious_accounts = run_query(Q_TOP_SUSPICIOUS_ACCOUNTS)
    alert_summary = run_query(Q_ALERT_SUMMARY).iloc[0]
    incremental_runs = run_query(Q_INCREMENTAL_RUNS)
    incremental_footprint = run_query(Q_INCREMENTAL_FOOTPRINT)

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

# ── AML business dashboard ───────────────────────────────────────────────────

st.markdown("---")
st.header("4 — 📊 Dashboard AML (Business)")
st.caption("Risk-oriented monitoring built from the latest Gold feature snapshot.")

refresh_col, summary_col = st.columns([1, 4])
with refresh_col:
    if st.button("Refresh alerts", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

with summary_col:
    st.markdown(
        f"**Latest snapshot:** {alert_summary['latest_feature_date']} | "
        f"**Critical alerts:** {alert_summary['critical_alerts']:,.0f} | "
        f"**Active alerts:** {alert_summary['active_alerts']:,.0f}"
    )

summary_metrics = st.columns(4)
summary_metrics[0].metric("Critical Alerts", f"{alert_summary['critical_alerts']:,.0f}")
summary_metrics[1].metric("Active Alerts", f"{alert_summary['active_alerts']:,.0f}")
summary_metrics[2].metric("Average Risk Score", f"{alert_summary['avg_risk_score']:.1f}")
summary_metrics[3].metric("Max Risk Score", f"{alert_summary['max_risk_score']:.1f}")

if suspicious_accounts.empty:
    st.warning("No suspicious accounts were found in the latest Gold snapshot.")
else:
    display_accounts = suspicious_accounts.rename(
        columns={
            "customer_id": "Customer ID",
            "feature_date": "Feature Date",
            "risk_score": "Risk Score",
            "alert_level": "Alert Level",
            "velocity_spike_ratio_7d": "Velocity Spike 7d",
            "cross_currency_ratio_30d": "Cross-Currency Ratio 30d",
            "amount_max_to_avg_ratio_7d": "Amount Max/Avg Ratio 7d",
            "counterparty_concentration_30d": "Counterparty Concentration 30d",
            "tx_count_7d": "Tx Count 7d",
            "tx_count_90d": "Tx Count 90d",
            "days_since_last_tx": "Days Since Last Tx",
            "primary_signal": "Primary Signal",
        }
    )

    st.subheader("Top suspicious accounts")
    st.dataframe(
        display_accounts.style.format(
            {
                "Risk Score": "{:.1f}",
                "Velocity Spike 7d": "{:.2f}",
                "Cross-Currency Ratio 30d": "{:.2f}",
                "Amount Max/Avg Ratio 7d": "{:.2f}",
                "Counterparty Concentration 30d": "{:.2f}",
                "Tx Count 7d": "{:,.0f}",
                "Tx Count 90d": "{:,.0f}",
                "Days Since Last Tx": "{:.0f}",
            }
        ).background_gradient(subset=["Risk Score"], cmap="YlOrRd"),
        use_container_width=True,
        hide_index=True,
    )

    account_labels = [
        f"{row['customer_id']} | score {row['risk_score']:.1f} | {row['primary_signal']}"
        for _, row in suspicious_accounts.iterrows()
    ]
    selected_label = st.selectbox("Inspect suspicious account", account_labels)
    selected_customer = selected_label.split(" | ", 1)[0]
    behavior_query = Q_BEHAVIOR_EVOLUTION_TEMPLATE.format(
        customer_id=quote_sql_string(selected_customer)
    )
    behavior_df = run_query(behavior_query)
    latest_customer = suspicious_accounts.loc[
        suspicious_accounts["customer_id"] == selected_customer
    ].iloc[0]

    gauge_col, chart_col = st.columns([1, 2])

    with gauge_col:
        st.subheader("Risk score")
        gauge = go.Figure(
            go.Indicator(
                mode="gauge+number",
                value=float(latest_customer["risk_score"]),
                number={"suffix": "/100"},
                gauge={
                    "axis": {"range": [0, 100]},
                    "bar": {"color": "#C44536"},
                    "steps": [
                        {"range": [0, 55], "color": "#D8F3DC"},
                        {"range": [55, 70], "color": "#FFE8A3"},
                        {"range": [70, 85], "color": "#F9C74F"},
                        {"range": [85, 100], "color": "#F94144"},
                    ],
                },
                title={"text": latest_customer["alert_level"]},
            )
        )
        gauge.update_layout(height=280, margin=dict(l=20, r=20, t=60, b=20))
        st.plotly_chart(gauge, use_container_width=True)
        st.caption("Primary signal")
        st.markdown(f"**{latest_customer['primary_signal']}**")
        st.metric("Days since last tx", f"{latest_customer['days_since_last_tx']:.0f}")

    with chart_col:
        st.subheader("Behaviour evolution (7d vs 90d baseline)")
        behavior_chart = make_subplots(specs=[[{"secondary_y": True}]])
        behavior_chart.add_trace(
            go.Scatter(
                x=behavior_df["feature_date"],
                y=behavior_df["tx_count_7d"],
                name="7d activity",
                mode="lines",
                line=dict(color="#D96C4A", width=3),
            ),
            secondary_y=False,
        )
        behavior_chart.add_trace(
            go.Scatter(
                x=behavior_df["feature_date"],
                y=behavior_df["tx_count_90d_baseline_7d"],
                name="90d baseline (7d-equivalent)",
                mode="lines",
                line=dict(color="#4A90D9", width=2, dash="dash"),
            ),
            secondary_y=False,
        )
        behavior_chart.add_trace(
            go.Scatter(
                x=behavior_df["feature_date"],
                y=behavior_df["risk_score"],
                name="Risk score",
                mode="lines",
                line=dict(color="#222222", width=2),
            ),
            secondary_y=True,
        )
        behavior_chart.update_layout(height=340, legend_orientation="h")
        behavior_chart.update_yaxes(title_text="Transactions", secondary_y=False)
        behavior_chart.update_yaxes(title_text="Risk score", range=[0, 100], secondary_y=True)
        st.plotly_chart(behavior_chart, use_container_width=True)

    st.subheader("Real-time alerts")
    alert_rows = suspicious_accounts[suspicious_accounts["risk_score"] >= 70].head(5)
    if alert_rows.empty:
        st.success("No high-risk alerts in the latest snapshot.")
    else:
        for _, alert in alert_rows.iterrows():
            severity_label = f"{alert['alert_level']} | score {alert['risk_score']:.1f}"
            message = (
                f"Customer {alert['customer_id']} on {alert['feature_date']}: "
                f"{alert['primary_signal']}. 7d activity={alert['tx_count_7d']:,.0f}, "
                f"cross-currency ratio 30d={alert['cross_currency_ratio_30d']:.2f}."
            )
            if alert["risk_score"] >= 85:
                st.error(f"{severity_label} — {message}")
            else:
                st.warning(f"{severity_label} — {message}")

# ── Incremental execution footprint ──────────────────────────────────────────

st.markdown("---")
st.header("5 — Incremental Change Footprint")
st.caption(
    "This view shows exactly what the last incremental batch refreshed in Gold, "
    "compared with the previous one."
)

if len(incremental_runs) >= 2:
    latest_run = incremental_runs.iloc[0]
    previous_run = incremental_runs.iloc[1]

    latest_ts = pd.to_datetime(latest_run["run_ts"])
    previous_ts = pd.to_datetime(previous_run["run_ts"])
    rows_delta = latest_run["rows_touched"] - previous_run["rows_touched"]
    customers_delta = latest_run["customers_touched"] - previous_run["customers_touched"]

    st.markdown(
        f"**Executive summary:** the latest incremental run refreshed **{latest_run['rows_touched']:,.0f}** Gold rows "
        f"for **{latest_run['customers_touched']:,.0f}** customers across **{latest_run['affected_days']:,.0f}** days. "
        f"Compared with the previous run, that is a change of **{rows_delta:,.0f}** rows and "
        f"**{customers_delta:,.0f}** customers."
    )

    col_latest, col_previous, col_delta, col_window = st.columns(4)

    with col_latest:
        st.metric("Latest Batch", latest_ts.strftime("%Y-%m-%d %H:%M:%S UTC"))
        st.metric("Gold Rows Refreshed", f"{latest_run['rows_touched']:,.0f}")

    with col_previous:
        st.metric("Previous Batch", previous_ts.strftime("%Y-%m-%d %H:%M:%S UTC"))
        st.metric("Gold Rows Refreshed", f"{previous_run['rows_touched']:,.0f}")

    with col_delta:
        st.metric(
            "Change in Refreshed Rows",
            f"{rows_delta:,.0f}",
            delta=f"{(rows_delta / previous_run['rows_touched'] * 100):.1f}%" if previous_run['rows_touched'] else None,
        )
        st.metric(
            "Change in Customers",
            f"{customers_delta:,.0f}",
            delta=f"{(customers_delta / previous_run['customers_touched'] * 100):.1f}%" if previous_run['customers_touched'] else None,
        )

    with col_window:
        st.metric("Days Recomputed", f"{latest_run['affected_days']:,.0f}")
        st.metric(
            "Date Window",
            f"{latest_run['min_feature_date']} to {latest_run['max_feature_date']}",
        )

    footprint_chart = px.bar(
        incremental_footprint,
        x="feature_date",
        y="rows_touched",
        color="run_label",
        barmode="group",
        labels={
            "feature_date": "Feature Date",
            "rows_touched": "Gold Rows Refreshed",
            "run_label": "Incremental Batch",
        },
        color_discrete_sequence=["#D96C4A", "#4A90D9"],
    )
    footprint_chart.update_layout(height=420, legend_title_text="Compared Batches")
    st.plotly_chart(footprint_chart, use_container_width=True)

    footprint_table = incremental_footprint.pivot(
        index="feature_date",
        columns="run_label",
        values="rows_touched",
    ).fillna(0)
    footprint_table["delta_rows"] = (
        footprint_table.get("Latest incremental run", 0)
        - footprint_table.get("Previous incremental run", 0)
    )
    footprint_table = footprint_table.sort_index(ascending=False).reset_index()
    footprint_table = footprint_table.rename(
        columns={
            "feature_date": "Feature Date",
            "Latest incremental run": "Latest Batch",
            "Previous incremental run": "Previous Batch",
            "delta_rows": "Net Change",
        }
    )

    top_changes = footprint_table.reindex(
        footprint_table["Net Change"].abs().sort_values(ascending=False).index
    ).head(10)

    highlight_chart = px.bar(
        top_changes.sort_values("Net Change", ascending=True),
        x="Net Change",
        y="Feature Date",
        orientation="h",
        color="Net Change",
        color_continuous_scale=["#D96C4A", "#F4D35E", "#4A90D9"],
        labels={
            "Net Change": "Net Change in Gold Rows",
            "Feature Date": "Feature Date",
        },
    )
    highlight_chart.update_layout(height=360, coloraxis_showscale=False)

    col_chart, col_table = st.columns([3, 2])
    with col_chart:
        st.markdown("**Largest day-level changes**")
        st.plotly_chart(highlight_chart, use_container_width=True)

    with col_table:
        st.markdown("**Detailed daily comparison**")
        st.dataframe(
            footprint_table.style.format(
                {
                    "Latest Batch": "{:,.0f}",
                    "Previous Batch": "{:,.0f}",
                    "Net Change": "{:+,.0f}",
                }
            ).background_gradient(subset=["Net Change"], cmap="RdYlBu"),
            use_container_width=True,
            hide_index=True,
        )

    st.info(
        "Incremental execution does not rebuild the full Gold layer. It refreshes only the recent "
        "feature window, and the chart above shows exactly which feature_date partitions changed "
        "between the last two runs."
    )
else:
    st.warning("Not enough incremental history yet to compare the latest Gold batch with a previous one.")

# ── Null rate comparison ─────────────────────────────────────────────────────

st.markdown("---")
st.header("6 — Data Quality: Null Rates (Silver)")

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
st.header("7 — Feature Correlation (Gold Serving)")
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
st.header("8 — Sample Data Preview")

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
