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

resource "google_project_service" "secretmanager" {
  project            = var.project_id
  service            = "secretmanager.googleapis.com"
  disable_on_destroy = false
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
  dataset_id                 = var.bq_bronze_dataset
  location                   = var.bq_location
  delete_contents_on_destroy = false
  description                = "AML Bronze layer — raw ingested data"

  labels = {
    layer   = "bronze"
    project = "anti-money-laundering"
  }
}

resource "google_bigquery_dataset" "silver" {
  dataset_id                 = var.bq_silver_dataset
  location                   = var.bq_location
  delete_contents_on_destroy = false
  description                = "AML Silver layer — cleaned and validated data (PySpark)"

  labels = {
    layer   = "silver"
    project = "anti-money-laundering"
  }
}

resource "google_bigquery_dataset" "gold" {
  dataset_id                 = var.bq_gold_dataset
  location                   = var.bq_location
  delete_contents_on_destroy = false
  description                = "AML Gold layer — aggregated and modelled data (dbt)"

  labels = {
    layer   = "gold"
    project = "anti-money-laundering"
  }
}

# ------------------------------------------------------------------ #
# Kestra — Service Account                                            #
# ------------------------------------------------------------------ #

resource "google_service_account" "kestra" {
  account_id   = "kestra-runner"
  display_name = "Kestra Pipeline Runner"
  description  = "Used by the Kestra VM to access GCS and BigQuery"
}

resource "google_project_iam_member" "kestra_storage" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.kestra.email}"
}

resource "google_project_iam_member" "kestra_bigquery" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.kestra.email}"
}

resource "google_secret_manager_secret" "kestra_basic_auth_username" {
  secret_id = var.kestra_basic_auth_username_secret_name

  depends_on = [google_project_service.secretmanager]

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret" "kestra_basic_auth_password" {
  secret_id = var.kestra_basic_auth_password_secret_name

  depends_on = [google_project_service.secretmanager]

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_iam_member" "kestra_basic_auth_username_accessor" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.kestra_basic_auth_username.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.kestra.email}"
}

resource "google_secret_manager_secret_iam_member" "kestra_basic_auth_password_accessor" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.kestra_basic_auth_password.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.kestra.email}"
}

# ------------------------------------------------------------------ #
# Kestra — Firewall                                                    #
# ------------------------------------------------------------------ #

resource "google_compute_firewall" "kestra" {
  name    = "allow-kestra-ui"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["8080"]
  }

  source_ranges = var.kestra_allowed_cidrs
  target_tags   = ["kestra"]

  description = "Allow access to Kestra UI on port 8080"
}

resource "google_compute_address" "kestra" {
  name   = "kestra-public-ip"
  region = var.region
}

# ------------------------------------------------------------------ #
# Kestra — Compute Instance                                            #
# ------------------------------------------------------------------ #

resource "google_compute_instance" "kestra" {
  name         = "kestra-server"
  machine_type = var.kestra_machine_type
  zone         = "${var.region}-a"

  tags = ["kestra"]

  boot_disk {
    initialize_params {
      image = "projects/debian-cloud/global/images/family/debian-12"
      size  = 50
      type  = "pd-ssd"
    }
  }

  network_interface {
    network = "default"
    access_config {
      nat_ip = google_compute_address.kestra.address
    }
  }

  service_account {
    email  = google_service_account.kestra.email
    scopes = ["cloud-platform"]
  }

  metadata = {
    serial-port-enable = "TRUE"
  }

  metadata_startup_script = templatefile("${path.module}/kestra_startup.sh.tftpl", {
    project_id                        = var.project_id
    kestra_basic_auth_enabled         = var.kestra_basic_auth_enabled
    kestra_basic_auth_username_secret = var.kestra_basic_auth_username_secret_name
    kestra_basic_auth_password_secret = var.kestra_basic_auth_password_secret_name
  })

  labels = {
    layer   = "orchestration"
    project = "anti-money-laundering"
  }

  allow_stopping_for_update = true
}
