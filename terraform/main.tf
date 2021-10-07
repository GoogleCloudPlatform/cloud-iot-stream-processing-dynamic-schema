
provider "google" {}

resource "google_project_service" "pubsub-apis" {
  project = var.google_project_id
  service = "pubsub.googleapis.com"

  disable_dependent_services = false
  disable_on_destroy         = false
}

resource "google_project_service" "dataflow-apis" {
  project = var.google_project_id
  service = "dataflow.googleapis.com"

  disable_dependent_services = false
  disable_on_destroy         = false
}

resource "google_project_service" "cloud-iot-apis" {
  project = var.google_project_id
  service = "cloudiot.googleapis.com"

  disable_dependent_services = false
  disable_on_destroy         = false
}

resource "google_pubsub_topic" "default-telemetry" {
  name    = "default-telemetry"
  project = var.google_project_id

  depends_on = [
    google_project_service.pubsub-apis
  ]
}

resource "google_cloudiot_registry" "device-registry" {
  name    = var.google_iot_registry_id
  project = var.google_project_id
  region = var.google_default_region

  depends_on = [
    google_project_service.cloud-iot-apis,
    google_project_service.pubsub-apis,
    google_pubsub_topic.default-telemetry
  ]

  event_notification_configs {
    pubsub_topic_name = google_pubsub_topic.default-telemetry.id
  }

  http_config = {
    http_enabled_state = "HTTP_ENABLED"
  }

  mqtt_config = {
    mqtt_enabled_state = "MQTT_ENABLED"
  }
}

locals {
  # BigQuery table schemas where the data is stored. One schema per message type.
  metadata_table_schemas = {
    for dataType in var.data_type_configuration:
      dataType.schema_key=>file(dataType.schema_path)
  }

  # BigQuery datasets where the data is stored. One dataset per message type.
  metadata_destination_datasets = {
    for dataType in var.data_type_configuration:
      dataType.dataset_key=>dataType.destination_dataset
  }

  # BigQuery tables where the data is stored. One table per message type.
  metadata_destination_tables = {
    for dataType in var.data_type_configuration:
      dataType.table_key=>dataType.destination_table
  }

  # Schema map configuration, one per message type.
  metadata_schema_maps = {
    for dataType in var.data_type_configuration:
      dataType.schema_map_key=>file(dataType.schema_map_path)
    if dataType.id != "unknown-message"
  }

  # Input data validation schemas, one per message type.
  metadata_input_data_schemas = {
    input-data-schemas = file(var.input_data_schemas_path)
  }

  # All the configurations are merged together and stored as metadata on the IoT Core device.
  iot_device_metadata = merge(local.metadata_table_schemas, local.metadata_destination_tables, local.metadata_destination_datasets, local.metadata_schema_maps, local.metadata_input_data_schemas)
}

resource "google_cloudiot_device" "iot-device" {
  name     = var.google_iot_device_id
  registry = google_cloudiot_registry.device-registry.id
  metadata = local.iot_device_metadata
}

resource "google_bigquery_dataset" "dataset" {
    dataset_id = var.google_bigquery_dataset_id
    location = var.google_bigquery_default_region
    project = var.google_project_id
}

resource "google_dataflow_job" "streaming-processing" {
    name = "iot-event-processor"
    template_gcs_path = "gs://${var.google_dataflow_default_bucket}/templates/iot-stream-processing"
    temp_gcs_location = "gs://${var.google_dataflow_default_bucket}/tmp_dir"
    project = var.google_project_id
    region = var.google_default_region
    zone = var.google_default_zone
    machine_type = "n1-standard-1"

    parameters = {
        streaming = true
        numWorkers = 1
        inputTopic = google_pubsub_topic.default-telemetry.id
    }

    depends_on = [
        google_pubsub_topic.default-telemetry,
        google_project_service.dataflow-apis,
        google_bigquery_dataset.dataset
    ]
}
