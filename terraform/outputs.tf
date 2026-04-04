output "bronze_bucket_url" {
  description = "GCS URI of the Bronze bucket"
  value       = "gs://${google_storage_bucket.bronze.name}"
}

output "silver_bucket_url" {
  description = "GCS URI of the Silver bucket"
  value       = "gs://${google_storage_bucket.silver.name}"
}

output "artifacts_bucket_url" {
  description = "GCS URI of the artifacts/temp bucket"
  value       = "gs://${google_storage_bucket.artifacts.name}"
}

output "bq_bronze_dataset" {
  description = "BigQuery Bronze dataset full ID"
  value       = "${var.project_id}.${google_bigquery_dataset.bronze.dataset_id}"
}

output "bq_silver_dataset" {
  description = "BigQuery Silver dataset full ID"
  value       = "${var.project_id}.${google_bigquery_dataset.silver.dataset_id}"
}

output "bq_gold_dataset" {
  description = "BigQuery Gold dataset full ID"
  value       = "${var.project_id}.${google_bigquery_dataset.gold.dataset_id}"
}

output "kestra_url" {
  description = "Kestra UI URL"
  value       = "http://${google_compute_address.kestra.address}:8080"
}

output "kestra_service_account" {
  description = "Email da service account do Kestra"
  value       = google_service_account.kestra.email
}

output "kestra_basic_auth_username_secret_name" {
  description = "Secret Manager secret name for the Kestra basic auth username"
  value       = google_secret_manager_secret.kestra_basic_auth_username.secret_id
}

output "kestra_basic_auth_password_secret_name" {
  description = "Secret Manager secret name for the Kestra basic auth password"
  value       = google_secret_manager_secret.kestra_basic_auth_password.secret_id
}
