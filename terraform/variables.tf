variable "project_id" {
  description = "GCP Project ID"
  type        = string
  default     = "anti-ml-data-engineering"
}

variable "region" {
  description = "GCP region for GCS buckets"
  type        = string
  default     = "us-central1"
}

variable "bq_location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "US"
}

# ---- GCS Buckets ----

variable "gcs_bronze_bucket" {
  description = "Name of the GCS Bronze bucket (raw Kaggle data)"
  type        = string
  default     = "anti-ml-data-engineering-bronze"
}

variable "gcs_silver_bucket" {
  description = "Name of the GCS Silver bucket (cleaned Parquet data)"
  type        = string
  default     = "anti-ml-data-engineering-silver"
}

variable "gcs_artifacts_bucket" {
  description = "Name of the GCS artifacts bucket (Spark temp files, logs)"
  type        = string
  default     = "anti-ml-data-engineering-artifacts"
}

# ---- BigQuery Datasets ----

variable "bq_bronze_dataset" {
  description = "BigQuery Bronze dataset ID"
  type        = string
  default     = "aml_bronze"
}

variable "bq_silver_dataset" {
  description = "BigQuery Silver dataset ID"
  type        = string
  default     = "aml_silver"
}

variable "bq_gold_dataset" {
  description = "BigQuery Gold dataset ID"
  type        = string
  default     = "aml_gold"
}
