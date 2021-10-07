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

TABLES=($(bq ls "${GOOGLE_CLOUD_PROJECT}:${BIGQUERY_DATASET_ID}" | awk '$2 == "TABLE" {print $1}'))
if [ ! -z "${TABLES}" ]; then
  for TABLE in "${TABLES[@]}"; do
    bq rm -t -q "${GOOGLE_CLOUD_PROJECT}:${BIGQUERY_DATASET_ID}.${TABLE}"
  done
fi

if command -v terraform >/dev/null 2>&1; then
  cd terraform/
  echo "Clean up resources created by terraform."
  if terraform destroy -auto-approve ; then
    if gsutil ls -b gs://"${DATAFLOW_TEMPLATE_BUCKET}" >/dev/null 2>&1; then
      echo "Delete ${DATAFLOW_TEMPLATE_BUCKET} Google Cloud Storage bucket."
      gsutil rm -r gs://${DATAFLOW_TEMPLATE_BUCKET}
    fi

    if gsutil ls -b gs://"${TF_STATE_BUCKET}" >/dev/null 2>&1; then
      echo "Delete ${TF_STATE_BUCKET} Google Cloud Storage bucket."
      gsutil rm -r gs://${TF_STATE_BUCKET}
    fi
  fi
  cd ../
else
  echo "Terraform is not install in the environment, re-run clean up from an environment with terraform installed."
fi







