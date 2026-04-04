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

# ---- Kestra VM ----

variable "kestra_machine_type" {
  description = "GCE machine type for the Kestra server"
  type        = string
  default     = "e2-standard-4"
}

variable "kestra_allowed_cidrs" {
  description = "List of CIDR ranges allowed to access Kestra UI (port 8080). Restrict to your IP in production."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "kestra_basic_auth_enabled" {
  description = "Enable Kestra basic auth on the VM bootstrap. Disable for recovery scenarios."
  type        = bool
  default     = true
}

variable "kestra_basic_auth_username_secret_name" {
  description = "Secret Manager secret name that stores the Kestra basic auth username."
  type        = string
  default     = "kestra-basic-auth-username"
}

variable "kestra_basic_auth_password_secret_name" {
  description = "Secret Manager secret name that stores the Kestra basic auth password."
  type        = string
  default     = "kestra-basic-auth-password"
}

