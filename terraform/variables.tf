variable "google_project_id" {}
variable "google_default_region" {}
variable "google_default_zone" {}
variable "google_iot_registry_id" {}
variable "google_iot_device_id" {}
variable "google_bigquery_default_region" {}
variable "google_dataflow_default_bucket" {}
variable "google_bigquery_dataset_id" {}
variable "input_data_schemas_path" {}
variable "data_type_configuration" {
  type = list(object({
    id = string
    schema_key = string
    dataset_key = string
    table_key = string
    schema_map_key = string
    schema_path = string
    destination_table = string
    destination_dataset = string
    schema_map_path = string
  }))
}