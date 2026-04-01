terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ------------------------------------------------------------------ #
# GCS Buckets                                                          #
# ------------------------------------------------------------------ #

resource "google_storage_bucket" "bronze" {
  name                        = var.gcs_bronze_bucket
  location                    = var.region
  force_destroy               = false
  uniform_bucket_level_access = true

  labels = {
    layer   = "bronze"
    project = "anti-money-laundering"
  }

  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_storage_bucket" "silver" {
  name                        = var.gcs_silver_bucket
  location                    = var.region
  force_destroy               = false
  uniform_bucket_level_access = true

  labels = {
    layer   = "silver"
    project = "anti-money-laundering"
  }
}

resource "google_storage_bucket" "artifacts" {
  name                        = var.gcs_artifacts_bucket
  location                    = var.region
  force_destroy               = false
  uniform_bucket_level_access = true

  labels = {
    layer   = "artifacts"
    project = "anti-money-laundering"
  }

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }
}

# ------------------------------------------------------------------ #
# BigQuery Datasets                                                    #
# ------------------------------------------------------------------ #

resource "google_bigquery_dataset" "bronze" {
  dataset_id                  = var.bq_bronze_dataset
  location                    = var.bq_location
  delete_contents_on_destroy  = false
  description                 = "AML Bronze layer — raw ingested data"

  labels = {
    layer   = "bronze"
    project = "anti-money-laundering"
  }
}

resource "google_bigquery_dataset" "silver" {
  dataset_id                  = var.bq_silver_dataset
  location                    = var.bq_location
  delete_contents_on_destroy  = false
  description                 = "AML Silver layer — cleaned and validated data (PySpark)"

  labels = {
    layer   = "silver"
    project = "anti-money-laundering"
  }
}

resource "google_bigquery_dataset" "gold" {
  dataset_id                  = var.bq_gold_dataset
  location                    = var.bq_location
  delete_contents_on_destroy  = false
  description                 = "AML Gold layer — aggregated and modelled data (dbt)"

  labels = {
    layer   = "gold"
    project = "anti-money-laundering"
  }
}
