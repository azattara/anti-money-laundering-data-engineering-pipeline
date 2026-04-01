# Anti Money Laundering — Data Engineering Pipeline

A production-ready **Medallion Architecture** (Bronze / Silver / Gold) data pipeline for the [IBM Transactions for Anti Money Laundering (AML)](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml) Kaggle dataset, orchestrated with **Kestra**, processed with **PySpark**, stored on **Google Cloud Storage** and **BigQuery**, and modelled with **dbt**.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Project Structure](#project-structure)
3. [Prerequisites](#prerequisites)
4. [Kaggle Token Setup](#kaggle-token-setup)
5. [GCP Authentication](#gcp-authentication)
6. [Provisioning Infrastructure (Terraform)](#provisioning-infrastructure-terraform)
7. [Starting Kestra (Docker Compose)](#starting-kestra-docker-compose)
8. [Running the Pipeline](#running-the-pipeline)
9. [Bronze / Silver / Gold Explained](#bronze--silver--gold-explained)
10. [dbt (Gold Layer)](#dbt-gold-layer)

---

## Architecture Overview

```
┌─────────────┐     ┌───────────────────────────────┐     ┌───────────────────────────────────┐
│  Kaggle API │────▶│  Bronze (GCS)                 │────▶│  Silver (PySpark → BigQuery/GCS)  │
│  IBM AML    │     │  gs://…-bronze/raw/*.csv       │     │  aml_silver.transactions          │
└─────────────┘     └───────────────────────────────┘     └────────────┬──────────────────────┘
                                                                        │
                                                                        ▼
                                                          ┌─────────────────────────────┐
                                                          │  Gold (dbt → BigQuery)      │
                                                          │  aml_gold.mart_transactions │
                                                          └─────────────────────────────┘

Orchestration: Kestra (aml_medallion_pipeline flow)
Infrastructure: Terraform (GCS buckets + BigQuery datasets)
```

---

## Project Structure

```
.
├── ingestion/
│   ├── kaggle_download.py        # Download IBM AML dataset from Kaggle
│   └── upload_to_gcs.py          # Upload raw files to GCS Bronze bucket
├── spark/
│   └── clean_aml_data.py         # PySpark Silver layer: clean → BigQuery
├── kestra/
│   └── aml_pipeline.yaml         # Kestra orchestration flow
├── terraform/
│   ├── main.tf                   # GCS buckets + BigQuery datasets
│   ├── variables.tf
│   └── outputs.tf
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml.example      # Copy to ~/.dbt/profiles.yml
│   └── models/
│       ├── bronze/stg_transactions_bronze.sql
│       ├── silver/stg_transactions_silver.sql
│       ├── gold/mart_transactions_gold.sql
│       └── schema.yml
├── docker-compose.yml            # Kestra + PostgreSQL
├── requirements.txt
├── .env.example                  # Copy to .env and fill in secrets
└── README.md
```

---

## Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Python | ≥ 3.10 | |
| Java | ≥ 11 | Required for PySpark |
| Apache Spark | ≥ 3.5 | |
| Terraform | ≥ 1.5 | |
| Docker + Docker Compose | ≥ 24 | For Kestra |
| dbt-bigquery | ≥ 1.8 | Optional for Gold layer |

Install Python dependencies:

```bash
pip install -r requirements.txt
```

---

## Kaggle Token Setup

1. Log in to [kaggle.com](https://www.kaggle.com) → **Account** → **Create New API Token** → downloads `kaggle.json`.
2. Choose **one** of the following methods:

**Option A — Environment variables (recommended for CI/CD and Kestra):**

```bash
export KAGGLE_USERNAME=your_username
export KAGGLE_KEY=your_api_key
```

**Option B — kaggle.json file:**

```bash
mkdir -p ~/.kaggle
mv ~/Downloads/kaggle.json ~/.kaggle/kaggle.json
chmod 600 ~/.kaggle/kaggle.json
```

> ⚠️ Never commit `kaggle.json` or your API key to version control. Both are listed in `.gitignore`.

---

## GCP Authentication

1. Create a service account in your GCP project (`anti-ml-data-engineering`) with the following roles:
   - `roles/storage.objectAdmin` (GCS read/write)
   - `roles/bigquery.dataEditor` (BigQuery write)
   - `roles/bigquery.jobUser` (BigQuery job execution)

2. Download the JSON key and set:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

3. Copy `.env.example` to `.env` and fill in all values:

```bash
cp .env.example .env
# edit .env with your values
```

---

## Provisioning Infrastructure (Terraform)

```bash
cd terraform

# Initialise providers
terraform init

# Preview changes
terraform plan -var="project_id=anti-ml-data-engineering"

# Apply (creates GCS buckets + BigQuery datasets)
terraform apply -var="project_id=anti-ml-data-engineering"
```

This creates:

| Resource | Name |
|----------|------|
| GCS bucket | `anti-ml-data-engineering-bronze` |
| GCS bucket | `anti-ml-data-engineering-silver` |
| GCS bucket | `anti-ml-data-engineering-artifacts` |
| BigQuery dataset | `aml_bronze` |
| BigQuery dataset | `aml_silver` |
| BigQuery dataset | `aml_gold` |

---

## Starting Kestra (Docker Compose)

```bash
# Ensure .env is populated
cp .env.example .env  # then edit

# Start Kestra + PostgreSQL
docker compose up -d

# Check logs
docker compose logs -f kestra
```

Kestra UI will be available at **http://localhost:8080**.

**Upload the pipeline flow:**

```bash
curl -X POST http://localhost:8080/api/v1/flows/import \
  -H "Content-Type: multipart/form-data" \
  -F "fileUpload=@kestra/aml_pipeline.yaml"
```

**Configure Kestra secrets** (in the UI: Settings → Secrets):

| Secret key | Value |
|------------|-------|
| `KAGGLE_USERNAME` | Your Kaggle username |
| `KAGGLE_KEY` | Your Kaggle API key |
| `GCP_SA_KEY_PATH` | Path to service account JSON inside the container |

---

## Running the Pipeline

### Option 1 — Via Kestra UI

1. Open http://localhost:8080
2. Navigate to **Flows** → `anti_money_laundering.aml_medallion_pipeline`
3. Click **Execute** and monitor each task.

### Option 2 — Run steps manually

**Bronze — Download from Kaggle:**

```bash
python ingestion/kaggle_download.py
```

**Bronze — Upload to GCS:**

```bash
python ingestion/upload_to_gcs.py
```

**Silver — PySpark cleaning job:**

```bash
spark-submit \
  --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0 \
  spark/clean_aml_data.py
```

**Gold — dbt models:**

```bash
cd dbt
cp profiles.yml.example ~/.dbt/profiles.yml
# edit ~/.dbt/profiles.yml with your credentials
dbt deps
dbt run
dbt test
```

---

## Bronze / Silver / Gold Explained

### 🥉 Bronze — Raw Data

- **What:** Raw CSV files downloaded directly from Kaggle, stored as-is in GCS.
- **Where:** `gs://anti-ml-data-engineering-bronze/raw/`
- **Why:** Preserves the original data for reprocessing and audit. No transformations applied.

### 🥈 Silver — Cleaned Data

- **What:** PySpark job reads Bronze CSVs, applies:
  - Column name standardisation (lowercase, underscores)
  - Deduplication (`dropDuplicates`)
  - Null filtering on critical columns (`from_id`, `to_id`, `amount`)
  - Type casting (`amount` → Double, `timestamp` → Timestamp)
  - Ingestion metadata column (`_ingested_at`)
- **Where:** BigQuery `aml_silver.transactions` + `gs://anti-ml-data-engineering-silver/cleaned/` (Parquet)
- **Connector:** [Spark BigQuery Connector](https://github.com/GoogleCloudDataproc/spark-bigquery-connector) (`spark-bigquery-with-dependencies_2.12:0.34.0`)

### 🥇 Gold — Aggregated / Modelled Data

- **What:** dbt models in BigQuery that join, aggregate, and enrich Silver data for analytics.
- **Where:** BigQuery `aml_gold.*`
- **Current models:**
  - `stg_transactions_silver` — deduplicated Silver view
  - `mart_transactions_gold` — sender-receiver aggregation (count, sum, avg, min, max amount)
- **Status:** Skeleton implemented; extend models as business requirements are refined.

---

## dbt (Gold Layer)

```bash
cd dbt

# Install dbt packages (if any defined in packages.yml)
dbt deps

# Compile models (dry run)
dbt compile

# Run models
dbt run --target prod

# Run tests
dbt test

# Generate and serve docs
dbt docs generate
dbt docs serve
```

Configure your BigQuery connection in `~/.dbt/profiles.yml` (use `dbt/profiles.yml.example` as a template).

---

## Security Notes

- `.env`, `kaggle.json`, `credentials.json`, and `*.tfvars` are all listed in `.gitignore`.
- Never commit secrets to version control.
- Use Kestra's built-in secret store for credentials in production.
- Rotate the GCP service account key regularly and restrict its IAM permissions to the minimum required.
