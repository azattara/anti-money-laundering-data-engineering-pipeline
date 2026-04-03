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
    access_config {}
  }

  service_account {
    email  = google_service_account.kestra.email
    scopes = ["cloud-platform"]
  }

  metadata = {
    startup-script = "#!/bin/bash\nset -e\ncurl -fsSL https://get.docker.com | sh\ndocker rm -f kestra 2>/dev/null || true\ndocker run -d --name kestra --restart unless-stopped -p 8080:8080 -v /var/kestra/data:/app/storage -e KESTRA_CONFIGURATION='datasources:\\n  h2:\\n    url: jdbc:h2:file:/app/storage/kestra;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=false\\n    username: kestra\\n    password: kestra\\n    driverClassName: org.h2.Driver\\nkestra:\\n  repository:\\n    type: h2\\n  queue:\\n    type: h2\\n  storage:\\n    type: local\\n    local:\\n      base-path: /app/storage\\n  tasks:\\n    tmp-dir:\\n      path: /tmp/kestra-wd/tmp\\n' kestra/kestra:latest server standalone\n"
  }

  labels = {
    layer   = "orchestration"
    project = "anti-money-laundering"
  }

  allow_stopping_for_update = true
}
