provider "google" {
  project = var.project_id
  region  = var.region
}

# BigQuery Dataset
resource "google_bigquery_dataset" "bq_dataset" {
  dataset_id = var.dataset_id
  location   = var.region
  description = "Dataset for Looker API data"
}

# BigQuery Table with Partitioning
resource "google_bigquery_table" "bq_table" {
  dataset_id = google_bigquery_dataset.bq_dataset.dataset_id
  table_id   = var.table_id
  schema     = file(var.schema_file)

  time_partitioning {
    type = "DAY"
    field = "history__created_time"  # Partition field
  }

}
