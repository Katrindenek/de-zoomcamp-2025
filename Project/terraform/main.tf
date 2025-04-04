provider "google" {
  project = var.project_id
  region  = var.region
}

# BigQuery dataset
resource "google_bigquery_dataset" "news_dataset" {
  dataset_id = "news_data"
  location   = var.location
}

# Optional: Service account for dlt to access BigQuery
resource "google_service_account" "dlt_account" {
  account_id   = "dlt-bigquery-sa"
  display_name = "Service Account for DLT"
}

# # IAM bindings to allow dlt access BigQuery
# resource "google_project_iam_member" "bq_access" {
#   project = var.project_id
#   role    = "roles/bigquery.dataEditor"
#   member  = "serviceAccount:${google_service_account.dlt_account.email}"
# }

# Assigning roles
locals {
  roles = [
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/bigquery.readSessionUser"
  ]
}

resource "google_project_iam_member" "dlt_account_roles" {
  for_each = toset(local.roles)
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.dlt_account.email}"
}

# Generate key
resource "google_service_account_key" "dlt_account_key" {
  service_account_id = google_service_account.dlt_account.name
}

# Save credentials JSON locally
resource "local_file" "dlt_account_credentials" {
  content  = base64decode(google_service_account_key.dlt_account_key.private_key)
  filename = "${path.module}/dlt-service-account.json"
}
