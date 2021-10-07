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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.solutions.common.IoTCoreMessageInfo;
import com.google.common.collect.ImmutableList;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Get - the destination field schemas - the destination table schemas from Cloud IoT Core metadata.
 * Previous fetched schemas are cached.
 */
public class TableSchemaLoader {
  private static final String TABLE_SCHEMA_METADATA_PREFIX = "table-schema-";
  private static final Map<String, TableSchema> schemaCache = new HashMap<>();
  private static final Map<String, Map<String, TableFieldSchema>> fieldMapCache = new HashMap<>();

  public static TableSchema getSchema(IoTCoreMessageInfo messageInfo) {
    final String cacheKey = GCPIoTCoreUtil.getDeviceCacheKeyWithMessageType(messageInfo);

    if (schemaCache.containsKey(cacheKey)) {
      return schemaCache.get(cacheKey);
    }

    String schemaStr =
        GCPIoTCoreUtil.getMetaDataEntry(
            messageInfo, TABLE_SCHEMA_METADATA_PREFIX + messageInfo.getMessageType());
    if (schemaStr == null) {
      throw new RuntimeException(String.format("No table scheme find for device: %s", cacheKey));
    }
    TableSchema schema = createSchema(schemaStr);
    schemaCache.put(cacheKey, schema);
    return schemaCache.get(cacheKey);
  }

  public static Map<String, TableFieldSchema> getFieldMap(IoTCoreMessageInfo messageInfo) {
    final String cacheKey = GCPIoTCoreUtil.getDeviceCacheKeyWithMessageType(messageInfo);

    if (fieldMapCache.containsKey(cacheKey)) {
      return fieldMapCache.get(cacheKey);
    }

    String schemaStr =
        GCPIoTCoreUtil.getMetaDataEntry(
            messageInfo, TABLE_SCHEMA_METADATA_PREFIX + messageInfo.getMessageType());
    if (schemaStr == null) {
      throw new RuntimeException(String.format("No table scheme find for device: %s", cacheKey));
    }

    JsonArray fields = new JsonParser().parse(schemaStr).getAsJsonArray();
    List<TableFieldSchema> fieldSchemas = createFieldSchemaList(fields);
    Map<String, TableFieldSchema> fieldMap = new HashMap<>();
    for (TableFieldSchema tableFieldSchema : fieldSchemas) {
      fieldMap.put(tableFieldSchema.getName(), tableFieldSchema);
    }
    fieldMapCache.put(cacheKey, fieldMap);
    return fieldMapCache.get(cacheKey);
  }

  public static void clearCache(IoTCoreMessageInfo messageInfo) {
    final String cacheKey = GCPIoTCoreUtil.getDeviceCacheKeyWithMessageType(messageInfo);
    schemaCache.remove(cacheKey);
    fieldMapCache.remove(cacheKey);
  }

  private static TableSchema createSchema(String schemaStr) {
    JsonArray fields = new JsonParser().parse(schemaStr).getAsJsonArray();
    List<TableFieldSchema> fieldSchemas = createFieldSchemaList(fields);
    return new TableSchema().setFields(ImmutableList.copyOf(fieldSchemas));
  }

  private static List<TableFieldSchema> createFieldSchemaList(JsonArray fields) {
    List<TableFieldSchema> fieldSchemas = new ArrayList<>();
    fields.forEach(
        field -> {
          JsonObject fieldObj = field.getAsJsonObject();
          TableFieldSchema tableFieldSchema =
              new TableFieldSchema()
                  .setName(fieldObj.get("name").getAsString())
                  .setType(fieldObj.get("type").getAsString())
                  .setMode(fieldObj.get("mode").getAsString());
          if ("RECORD".equalsIgnoreCase(fieldObj.get("type").getAsString())) {
            if (fieldObj.has("fields")) {
              tableFieldSchema.setFields(
                  createFieldSchemaList(fieldObj.get("fields").getAsJsonArray()));
            }
          }
          fieldSchemas.add(tableFieldSchema);
        });
    return fieldSchemas;
  }
}
