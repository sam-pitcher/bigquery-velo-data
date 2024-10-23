output "dataset_id" {
  value       = google_bigquery_dataset.bq_dataset.dataset_id
  description = "The dataset ID for the created BigQuery dataset"
}

output "table_id" {
  value       = google_bigquery_table.bq_table.table_id
  description = "The table ID for the created BigQuery table"
}
