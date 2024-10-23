variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "The region to deploy resources in"
  default     = "US"
}

variable "dataset_id" {
  description = "BigQuery Dataset ID"
  type        = string
}

variable "table_id" {
  description = "BigQuery Table ID"
  type        = string
}

variable "schema_file" {
  description = "Path to the JSON schema file for the BigQuery table"
  type        = string
}