/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.solutions.utils;

import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.solutions.common.IoTCoreMessageInfo;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;

/**
 * Get the table destinations from Cloud IoT Core metadata and create TableDestination object from
 * the configuration. Previous fetched destinations are cached.
 */
public class TableDestinationLoader {
  private static final String DESTINATION_TABLE_METADATA_PREFIX = "destination-table-";
  private static final String DESTINATION_DATASET_METADATA_PREFIX = "destination-dataset-";
  private static final Map<String, TableDestination> destinationCache = new HashMap<>();

  public static TableDestination getDestination(IoTCoreMessageInfo messageInfo) {
    final String cacheKey = GCPIoTCoreUtil.getDeviceCacheKeyWithMessageType(messageInfo);
    if (destinationCache.containsKey(cacheKey)) {
      return destinationCache.get(cacheKey);
    }

    String table =
        GCPIoTCoreUtil.getMetaDataEntry(
            messageInfo, DESTINATION_TABLE_METADATA_PREFIX + messageInfo.getMessageType());
    if (table == null) {
      throw new RuntimeException(
          String.format("No destination table find for device: %s", cacheKey));
    }

    String dataset =
        GCPIoTCoreUtil.getMetaDataEntry(
            messageInfo, DESTINATION_DATASET_METADATA_PREFIX + messageInfo.getMessageType());
    if (dataset == null) {
      throw new RuntimeException(
          String.format("No destination dataset find for device: %s", cacheKey));
    }

    TableDestination tableDestination =
        new TableDestination(
            new TableReference()
                .setProjectId(messageInfo.getProjectId())
                .setDatasetId(dataset)
                .setTableId(table),
            "Dynamically loaded destination");

    destinationCache.put(cacheKey, tableDestination);

    return destinationCache.get(cacheKey);
  }

  public static void clearCache(IoTCoreMessageInfo messageInfo) {
    final String cacheKey = GCPIoTCoreUtil.getDeviceCacheKeyWithMessageType(messageInfo);
    destinationCache.remove(cacheKey);
  }
}
