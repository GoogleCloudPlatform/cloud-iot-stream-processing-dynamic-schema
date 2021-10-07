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

import com.google.cloud.solutions.common.IoTCoreMessageInfo;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.HashMap;
import java.util.Map;

/**
 * Get the schema map configurations from Cloud IoT Core metadata and parse it into JsonObject.
 * Previous fetched configurations are cached
 */
public class SchemaMapLoader {
  private static final Map<String, JsonObject> mapCache = new HashMap<>();
  private static final String SCHEMA_MAP_METADATA_PREFIX = "schema-map-";

  public static JsonObject getSchemaMap(IoTCoreMessageInfo messageInfo) {
    final String cacheKey = GCPIoTCoreUtil.getDeviceCacheKeyWithMessageType(messageInfo);

    if (mapCache.containsKey(cacheKey)) {
      return mapCache.get(cacheKey);
    }

    String mapStr =
        GCPIoTCoreUtil.getMetaDataEntry(
            messageInfo, SCHEMA_MAP_METADATA_PREFIX + messageInfo.getMessageType());
    if (mapStr == null) {
      throw new RuntimeException(String.format("No schema map find for device: %s", cacheKey));
    }
    JsonObject map = new JsonParser().parse(mapStr).getAsJsonObject();
    mapCache.put(cacheKey, map);
    return mapCache.get(cacheKey);
  }

  public static void clearCache(IoTCoreMessageInfo messageInfo) {
    final String cacheKey = GCPIoTCoreUtil.getDeviceCacheKeyWithMessageType(messageInfo);
    mapCache.remove(cacheKey);
  }
}
