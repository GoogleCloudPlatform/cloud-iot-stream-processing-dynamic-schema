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


if [ -z "${IOT_DEVICE_ID}" ]; then
    echo "Setting environment variables."
    . ../scripts/set-env.sh
fi

CLIENT_CONFIG_FILE=cloud_config.ini
if [ ! -f "${CLIENT_CONFIG_FILE}" ]; then
    echo "Generate client config file ${CLIENT_CONFIG_FILE}."
    envsubst < cloud_config_template.ini > ${CLIENT_CONFIG_FILE}
fi

GOOGLE_CERT_PEM=roots.pem
if [ ! -f "${GOOGLE_CERT_PEM}" ]; then
    echo "Download Google Root Certificate ${GOOGLE_CERT_PEM}."
    curl https://pki.goog/roots.pem --output roots.pem
fi


IOT_DEVICE_PUB_KEY=rsa_public.pem
IOT_DEVICE_PRI_KEY=rsa_private.pem
if [ ! -f "${IOT_DEVICE_PUB_KEY}" ] || [ ! -f "${IOT_DEVICE_PRI_KEY}" ]; then
    echo "Generate device key pair."

    openssl genpkey -algorithm RSA -out ${IOT_DEVICE_PRI_KEY} -pkeyopt rsa_keygen_bits:2048 && \
    openssl rsa -in ${IOT_DEVICE_PRI_KEY} -pubout -out ${IOT_DEVICE_PUB_KEY}

    DEVICE_CREDENTIAL_INDEX=$(gcloud iot devices credentials list \
        --project=${GOOGLE_CLOUD_PROJECT} \
        --region=${GOOGLE_CLOUD_REGION} \
        --registry=${IOT_REGISTRY_ID} \
        --device=${IOT_DEVICE_ID} | awk '$2 == "RSA_PEM" {print $1}')

    if [ ! -z ${DEVICE_CREDENTIAL_INDEX} ]; then
        gcloud iot devices credentials delete ${DEVICE_CREDENTIAL_INDEX} \
            --project=${GOOGLE_CLOUD_PROJECT} \
            --region=${GOOGLE_CLOUD_REGION} \
            --registry=${IOT_REGISTRY_ID} \
            --device=${IOT_DEVICE_ID} \
            --quiet
    fi
fi

DEVICE_CREDENTIAL_INDEX=$(gcloud iot devices credentials list \
    --project=${GOOGLE_CLOUD_PROJECT} \
    --region=${GOOGLE_CLOUD_REGION} \
    --registry=${IOT_REGISTRY_ID} \
    --device=${IOT_DEVICE_ID} | awk '$2 == "RSA_PEM" {print $1}')

if [ -z ${DEVICE_CREDENTIAL_INDEX} ]; then
    gcloud iot devices credentials create \
      --project=${GOOGLE_CLOUD_PROJECT} \
      --region=${GOOGLE_CLOUD_REGION} \
      --registry=${IOT_REGISTRY_ID} \
      --device=${IOT_DEVICE_ID} \
      --path=${IOT_DEVICE_PUB_KEY} \
      --type=rsa-pem
fi

VENV_DIR=venv
if [ ! -d "${VENV_DIR}" ]; then
    echo "Set up python virtual environment."
    python3 -m venv ${VENV_DIR} && source ${VENV_DIR}/bin/activate && CRYPTOGRAPHY_DONT_BUILD_RUST=1 pip3 install -r requirements.txt && deactivate
fi

source ${VENV_DIR}/bin/activate && python3 sim_device.py ${1}

deactivate
