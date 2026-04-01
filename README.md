# Anti Money Laundering — Data Engineering Pipeline

A production-ready **Medallion Architecture** (Bronze / Silver / Gold) data pipeline for the [IBM Transactions for Anti Money Laundering (AML)](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml) Kaggle dataset, orchestrated with **Kestra**, processed with **PySpark**, stored on **Google Cloud Storage** and **BigQuery**, and modelled with **dbt**.

The **Gold layer is a Feature Store** with temporal feature engineering (rolling windows), incremental materialisation, BigQuery partitioning/clustering, and rule-based feature selection — designed for AML model training and scoring.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Project Structure](#project-structure)
3. [Gold Layer as Feature Store](#gold-layer-as-feature-store)
4. [Temporal Feature Engineering (Rolling Windows)](#temporal-feature-engineering-rolling-windows)
5. [Incremental Strategy (Silver & Gold)](#incremental-strategy-silver--gold)
6. [90-Day Lookback and Implications](#90-day-lookback-and-implications)
7. [BigQuery Partitioning & Clustering](#bigquery-partitioning--clustering)
8. [Feature Selection (Rule-Based)](#feature-selection-rule-based)
9. [Prerequisites](#prerequisites)
10. [Kaggle Token Setup](#kaggle-token-setup)
11. [GCP Authentication](#gcp-authentication)
12. [Provisioning Infrastructure (Terraform)](#provisioning-infrastructure-terraform)
13. [Starting Kestra (Docker Compose)](#starting-kestra-docker-compose)
14. [Running the Pipeline](#running-the-pipeline)
15. [Running dbt Incrementally](#running-dbt-incrementally)
16. [dbt Model Reference](#dbt-model-reference)
17. [Adding / Removing Features](#adding--removing-features)

---

## Architecture Overview

```
┌─────────────┐   ┌───────────────────────────────────────────────────┐
│  Kaggle API │──▶│  Bronze (GCS — date-partitioned)                  │
│  IBM AML    │   │  gs://…-bronze/partitions/YYYY-MM-DD/*.csv        │
└─────────────┘   └───────────────────────┬───────────────────────────┘
                                          │ PySpark (incremental)
                                          ▼
                  ┌───────────────────────────────────────────────────┐
                  │  Silver (BigQuery + GCS)                          │
                  │  aml_silver.transactions                          │
                  │  fct_transactions_silver  (dbt — typed/clean)     │
                  └───────────────────────┬───────────────────────────┘
                                          │ dbt (incremental, 90-day lookback)
                                          ▼
                  ┌───────────────────────────────────────────────────┐
                  │  Gold — Feature Store (BigQuery)                  │
                  │  aml_gold.mart_customer_features_gold             │
                  │      └─▶ aml_gold.feature_store_customer_features │
                  │  Grain: (customer_id, feature_date)               │
                  │  Partition: feature_date  │  Cluster: customer_id │
                  └───────────────────────────────────────────────────┘

Orchestration: Kestra (aml_medallion_pipeline flow)
Infrastructure: Terraform (GCS buckets + BigQuery datasets)
Manifest/checkpoint: aml_ops.ingestion_manifest (BigQuery)
```

---

## Project Structure

```
.
├── ingestion/
│   ├── kaggle_download.py             # Download IBM AML dataset from Kaggle
│   └── upload_to_gcs.py               # Upload raw files to GCS Bronze bucket
├── spark/
│   └── clean_aml_data.py              # PySpark Silver layer (incremental)
├── kestra/
│   └── aml_pipeline.yaml              # Kestra orchestration flow
├── terraform/
│   ├── main.tf                        # GCS buckets + BigQuery datasets
│   ├── variables.tf
│   └── outputs.tf
├── dbt/
│   ├── dbt_project.yml                # Project config (incremental Gold defaults)
│   ├── profiles.yml.example           # Copy to ~/.dbt/profiles.yml
│   ├── macros/
│   │   └── rolling_window.sql         # Reusable rolling-window macro
│   └── models/
│       ├── schema.yml                 # Full model + column documentation
│       ├── bronze/
│       │   └── stg_transactions_bronze.sql
│       ├── silver/
│       │   ├── stg_transactions_silver.sql   (legacy)
│       │   └── fct_transactions_silver.sql   ← canonical Silver source
│       └── gold/
│           ├── mart_transactions_gold.sql            (legacy)
│           ├── mart_customer_features_gold.sql       ← feature compute layer
│           └── feature_store_customer_features.sql   ← feature serving layer
├── docker-compose.yml                 # Kestra + Postgres (local dev)
├── requirements.txt
├── .env.example
└── .gitignore
```

---

## Gold Layer as Feature Store

The Gold layer is treated as a **lightweight feature store** rather than a simple aggregation layer.

| Concept | Implementation |
|---|---|
| **Entity** | `customer_id` (sender of the transaction) |
| **Time key** | `feature_date` = `DATE(timestamp)` |
| **Grain** | One row per `(customer_id, feature_date)` |
| **Versioning** | `feature_version` column (e.g. `'1.0'`) in every row |
| **Lineage** | `source_model` column records which dbt model produced the row |
| **Freshness** | `feature_created_at` timestamp on every row |
| **Serving table** | `aml_gold.feature_store_customer_features` |
| **Compute table** | `aml_gold.mart_customer_features_gold` |

The two-layer approach (compute → serving) lets you:
- Compute all candidate features in `mart_customer_features_gold`
- Publish only approved features in `feature_store_customer_features` after rule-based selection
- Evolve features without breaking downstream consumers

---

## Temporal Feature Engineering (Rolling Windows)

Instead of static per-customer aggregations, the Gold layer models **behaviour over time** using rolling windows anchored on `feature_date`.

| Feature group | Windows | AML signal |
|---|---|---|
| **Frequency** (`tx_count_*d`) | 7 / 30 / 90 days | Velocity of activity |
| **Monetary sum** (`tx_amount_sum_*d`) | 7 / 30 / 90 days | Volume of funds moved |
| **Monetary avg/max** | 7 / 30 / 90 days | Structuring detection |
| **Counterparty diversity** (`unique_counterparties_*d`) | 7 / 30 / 90 days | Fan-out / layering |
| **Cross-currency count & ratio** | 30 / 90 days | Jurisdiction hopping |
| **Payment-format diversity** | 90 days | Multi-rail obfuscation |
| **Recency** (`days_since_last_tx`) | — | Dormancy / burst patterns |
| **Velocity spike ratio** | 7d vs 30d avg | Sudden activity bursts |
| **Amount max/avg ratio** | 7 days | Single large transaction vs baseline |
| **Counterparty concentration** | 30 days | Few counterparties, many transactions |

Windows are implemented with a reusable dbt macro (`macros/rolling_window.sql`) that emits BigQuery `RANGE BETWEEN` window functions anchored on `unix_date(feature_date)` — correctly handling gaps in the daily time series.

---

## Incremental Strategy (Silver & Gold)

### Bronze → Silver (PySpark)

- Bronze files are uploaded to GCS under **date-partitioned prefixes**:
  ```
  gs://<bronze-bucket>/partitions/YYYY-MM-DD/*.csv
  ```
- The Spark job (`spark/clean_aml_data.py`) accepts `--partition-date YYYY-MM-DD` and processes only that prefix.
- A **manifest table** (`aml_ops.ingestion_manifest`) in BigQuery records each successfully processed partition, preventing double-processing.
- Kestra passes `inputs.partition_date` (defaulting to yesterday) to the Spark job at runtime.

**Full-refresh** (one-time or recovery):
```bash
spark-submit spark/clean_aml_data.py --full-refresh
```

**Incremental** (daily, via Kestra or manually):
```bash
spark-submit spark/clean_aml_data.py --partition-date 2024-03-15
```

### Silver → Gold (dbt)

The Gold models are `materialized='incremental'` with `incremental_strategy='merge'` and `unique_key=['customer_id', 'feature_date']`.

On each incremental run, **only the last 90 days are recomputed**:

```sql
-- Applied automatically in mart_customer_features_gold when is_incremental()
where feature_date >= date_sub(
    (select max(feature_date) from {{ this }}),
    interval 90 day
)
```

This avoids a full historical recomputation while ensuring all rolling windows (up to 90 days) remain accurate for newly arrived data.

---

## 90-Day Lookback and Implications

The **90-day lookback** is the standard AML monitoring window recommended by FATF/FinCEN for suspicious activity reporting.

**Why 90 days?**
- The longest rolling window feature is `tx_count_90d`, `tx_amount_sum_90d`, etc.
- When new transactions arrive for date `T`, the feature values for dates `T-89` through `T` may be affected.
- Re-processing only the 90-day window is the minimal correct recomputation.

**Storage implications:**
- Each incremental dbt run merges ~90 days × N customers rows into the Gold table.
- BigQuery partition pruning (on `feature_date`) ensures only affected partitions are scanned/updated.
- Older partitions are never touched unless a `--full-refresh` is explicitly requested.

**Choosing a different lookback:**
- Change the `interval 90 day` in both `mart_customer_features_gold.sql` and `feature_store_customer_features.sql`.
- Ensure the window size matches the longest rolling window feature you compute.

---

## BigQuery Partitioning & Clustering

All Gold models are configured with explicit BigQuery optimisations in `dbt_project.yml` (and overridable per model):

```yaml
# dbt/dbt_project.yml
gold:
  +partition_by:
    field: feature_date
    data_type: date
    granularity: day
  +cluster_by:
    - customer_id
```

| Setting | Value | Benefit |
|---|---|---|
| `partition_by` | `feature_date` (DATE) | Prunes old partitions on incremental merge; cheaper queries filtered by date |
| `cluster_by` | `customer_id` | Speeds up single-customer lookups from scoring jobs |

**Cost estimate**: With partitioning, a query filtering `WHERE feature_date = '2024-03-15'` scans only one day's data regardless of historical size.

---

## Feature Selection (Rule-Based)

The `feature_store_customer_features` model is the **selection layer** — it applies four explainable rules before publishing features to consumers:

| Rule | Description | Example exclusion |
|---|---|---|
| **RULE-1** (Null threshold) | Features with expected null-rate > 30% are coalesced to a sentinel value or excluded | `days_since_last_tx` → coalesced to `-1` on first day |
| **RULE-2** (Low variance) | Features that are trivially constant add no signal | Raw 1-day amount std excluded in favour of rolling equivalents |
| **RULE-3** (Redundancy) | Highly correlated features are reduced to one representative | Ratio features capture the relationship; raw pairs may be dropped |
| **RULE-4** (Leakage) | Target / label columns are never published | `is_laundering` excluded from all feature store outputs |

### Adding a new feature

1. Compute it in `mart_customer_features_gold.sql` (add to the appropriate CTE).
2. Add it to the SELECT in `feature_store_customer_features.sql` with a comment stating which rule it passes.
3. Document it in `dbt/models/schema.yml` under both models.
4. Run `dbt run --select mart_customer_features_gold feature_store_customer_features`.

### Removing a feature

1. Remove it from the SELECT in `feature_store_customer_features.sql`.
2. Mark it as deprecated in `schema.yml` with removal date and reason.
3. Optionally remove the computation from `mart_customer_features_gold.sql` after a deprecation period.

---

## Prerequisites

| Tool | Version | Purpose |
|---|---|---|
| Python | ≥ 3.9 | Scripts + dbt |
| Docker & Docker Compose | ≥ 24 | Kestra local |
| Terraform | ≥ 1.5 | GCP provisioning |
| `gcloud` CLI | any | GCP auth |
| Apache Spark | ≥ 3.4 | Silver job |
| dbt-bigquery | ≥ 1.7 | Gold models |
| Kaggle account | — | Dataset download |

Install Python dependencies:
```bash
pip install -r requirements.txt
```

---

## Kaggle Token Setup

1. Go to [kaggle.com/settings](https://www.kaggle.com/settings) → **API** → **Create New Token** → downloads `kaggle.json`.
2. Place it at `~/.config/kaggle/kaggle.json` (Linux/Mac) or `%USERPROFILE%\.kaggle\kaggle.json` (Windows).
3. Or set environment variables:
   ```bash
   export KAGGLE_USERNAME=your_username
   export KAGGLE_KEY=your_api_key
   ```

---

## GCP Authentication

```bash
# Option A: user credentials (local dev)
gcloud auth application-default login

# Option B: service account (CI/production)
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

Copy `.env.example` to `.env` and fill in your values:
```bash
cp .env.example .env
```

---

## Provisioning Infrastructure (Terraform)

```bash
cd terraform
terraform init
terraform plan -var="project_id=anti-ml-data-engineering"
terraform apply -var="project_id=anti-ml-data-engineering"
```

Resources created:
- `anti-ml-data-engineering-bronze` (GCS bucket)
- `anti-ml-data-engineering-silver` (GCS bucket)
- `anti-ml-data-engineering-artifacts` (GCS bucket — Spark temp)
- `aml_bronze`, `aml_silver`, `aml_gold`, `aml_ops` (BigQuery datasets)

---

## Starting Kestra (Docker Compose)

```bash
docker compose up -d
```

Open [http://localhost:8080](http://localhost:8080) and import the flow:
1. Go to **Flows** → **Create**.
2. Paste or upload `kestra/aml_pipeline.yaml`.

Configure secrets in Kestra UI (**Settings → Secrets**):
- `KAGGLE_USERNAME`, `KAGGLE_KEY`
- `GCP_SA_KEY_PATH`
- `GCS_ARTIFACTS_BUCKET`

---

## Running the Pipeline

### Via Kestra UI

1. Open the `aml_medallion_pipeline` flow.
2. Click **Execute**.
3. Set inputs:
   - `partition_date`: `YYYY-MM-DD` (default: yesterday)
   - `run_mode`: `INCREMENTAL` or `FULL_REFRESH`

### Manually (step by step)

```bash
# 1. Download from Kaggle
python ingestion/kaggle_download.py

# 2. Upload to GCS
python ingestion/upload_to_gcs.py

# 3. Run Spark Silver job (incremental for a specific date)
spark-submit \
  --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0 \
  spark/clean_aml_data.py --partition-date 2024-03-15

# 4. Run dbt Gold (incremental)
cd dbt && dbt run --select fct_transactions_silver+ 

# 5. Run dbt tests
dbt test --select fct_transactions_silver mart_customer_features_gold feature_store_customer_features
```

---

## Running dbt Incrementally

**First run (full history):**
```bash
cd dbt
dbt run --full-refresh --select fct_transactions_silver mart_customer_features_gold feature_store_customer_features
```

**Daily incremental run (only processes changed data, 90-day window):**
```bash
cd dbt
dbt run --select fct_transactions_silver mart_customer_features_gold feature_store_customer_features
```

**Run a single model:**
```bash
dbt run --select mart_customer_features_gold
```

**Run with a specific target date** (useful for backfills — Silver side):
```bash
spark-submit spark/clean_aml_data.py --partition-date 2024-01-15
dbt run --select mart_customer_features_gold feature_store_customer_features
```

---

## dbt Model Reference

| Model | Layer | Grain | Materialization | Notes |
|---|---|---|---|---|
| `stg_transactions_bronze` | Bronze | transaction | view | Thin passthrough |
| `stg_transactions_silver` | Silver | transaction | table | Legacy; use `fct_transactions_silver` |
| `fct_transactions_silver` | Silver | transaction | table | Canonical typed Silver source |
| `mart_transactions_gold` | Gold | (from_id, to_id) | table | Legacy aggregation |
| `mart_customer_features_gold` | Gold | (customer_id, feature_date) | incremental | Full feature computation |
| `feature_store_customer_features` | Gold | (customer_id, feature_date) | incremental | Published feature store (rule-selected) |

---

## Licence

MIT
