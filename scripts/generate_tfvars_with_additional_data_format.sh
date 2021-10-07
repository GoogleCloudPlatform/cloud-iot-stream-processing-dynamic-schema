#!/usr/bin/env sh

# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if [ -z "${BIGQUERY_DATASET_ID}" ]; then
    echo "Setting environment variables."
    . scripts/set-env.sh
fi

TERRAFORM_VARIABLE_FILE_PATH=terraform/new_data_format.tfvars
if [ ! -f "${TERRAFORM_VARIABLE_FILE_PATH}" ]; then
  CONFIGS_DIR_RELATIVE_PATH="../data-configs/"
  UNKNOWN_MESSAGE_TYPE_ID="unknown-message"
  EDGEX_MESSAGE_TYPE_ID="edgex"
  PERSON_DETECTION_MESSAGE_TYPE_ID="person-detection"

  tee "${TERRAFORM_VARIABLE_FILE_PATH}" <<EOF
input_data_schemas_path="${CONFIGS_DIR_RELATIVE_PATH}input-data-schema-update.json"
data_type_configuration = [
  {
    id = "${UNKNOWN_MESSAGE_TYPE_ID}"
    schema_key = "table-schema-${UNKNOWN_MESSAGE_TYPE_ID}"
    dataset_key = "destination-dataset-${UNKNOWN_MESSAGE_TYPE_ID}"
    table_key = "destination-table-${UNKNOWN_MESSAGE_TYPE_ID}"
    schema_map_key = ""
    schema_path = "${CONFIGS_DIR_RELATIVE_PATH}${UNKNOWN_MESSAGE_TYPE_ID}-table-schema.json"
    destination_table = "${BIGQUERY_UNKNOWN_MESSAGE_TABLE_ID}"
    destination_dataset = "${BIGQUERY_DATASET_ID}"
    schema_map_path = ""
  },
  {
    id = "${EDGEX_MESSAGE_TYPE_ID}"
    schema_key = "table-schema-${EDGEX_MESSAGE_TYPE_ID}"
    dataset_key = "destination-dataset-${EDGEX_MESSAGE_TYPE_ID}"
    table_key = "destination-table-${EDGEX_MESSAGE_TYPE_ID}"
    schema_map_key = "schema-map-${EDGEX_MESSAGE_TYPE_ID}"
    schema_path = "${CONFIGS_DIR_RELATIVE_PATH}${EDGEX_MESSAGE_TYPE_ID}-table-schema.json"
    destination_table = "${BIGQUERY_METRICS_TABLE_ID}"
    destination_dataset = "${BIGQUERY_DATASET_ID}"
    schema_map_path = "${CONFIGS_DIR_RELATIVE_PATH}${EDGEX_MESSAGE_TYPE_ID}-schema-mapping.json"
  },
  {
    id = "${PERSON_DETECTION_MESSAGE_TYPE_ID}"
    schema_key = "table-schema-${PERSON_DETECTION_MESSAGE_TYPE_ID}"
    dataset_key = "destination-dataset-${PERSON_DETECTION_MESSAGE_TYPE_ID}"
    table_key = "destination-table-${PERSON_DETECTION_MESSAGE_TYPE_ID}"
    schema_map_key = "schema-map-${PERSON_DETECTION_MESSAGE_TYPE_ID}"
    schema_path = "${CONFIGS_DIR_RELATIVE_PATH}${PERSON_DETECTION_MESSAGE_TYPE_ID}-table-schema.json"
    destination_table = "${BIGQUERY_PERSON_DETECTION_TABLE_ID}"
    destination_dataset = "${BIGQUERY_DATASET_ID}"
    schema_map_path = "${CONFIGS_DIR_RELATIVE_PATH}${PERSON_DETECTION_MESSAGE_TYPE_ID}-schema-mapping.json"
  }
]
EOF
fi

