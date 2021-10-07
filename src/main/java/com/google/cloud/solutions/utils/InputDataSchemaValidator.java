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
import com.google.cloud.solutions.common.PubSubMessageWithMessageInfo;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;

public class InputDataSchemaValidator
    implements SerializableFunction<PubSubMessageWithMessageInfo, Boolean> {

  private static final long serialVersionUID = -2659650110307165972L;
  private static final String INPUT_DATA_SCHEMA_META_DATA_KEY = "input-data-schemas";
  private static final String SCHEMA_FIELD = "schema";
  private static final String DATA_TYPE_FIELD = "dataType";
  private boolean validationNotFailedSoFar = true;
  private final Map<String, Map<String, Schema>> schemaCache;

  public InputDataSchemaValidator() {
    this.schemaCache = new HashMap<>();
  }

  @Override
  public Boolean apply(PubSubMessageWithMessageInfo messageWithInfo) {
    Map<String, Schema> jsonValidationSchemas = getSchemaList(messageWithInfo);
    if (jsonValidationSchemas == null) { // not schema available to validate against
      return false;
    }
    JSONObject messageToValidate;

    try {
      messageToValidate =
          new JSONObject(new JSONTokener(new ByteArrayInputStream(messageWithInfo.getPayload())));
    } catch (Exception e) { // not valid json
      return false;
    }

    for (Map.Entry<String, Schema> entry : jsonValidationSchemas.entrySet()) {
      try {
        entry.getValue().validate(messageToValidate);
        messageWithInfo.getMessageInfo().setMessageType(entry.getKey());
        return validateMessageTypeConfigurations(messageWithInfo.getMessageInfo());
      } catch (ValidationException ignored) {
      } // do not match schema requirement
    }

    // When new message type is added as device metadata, the initial validation a message with the
    // new message type will fail. The validator need to clear its cache and reload validation
    // schemas from device metadata store. By having the validationNotFailedSoFar, we'll avoid the
    // case where invalid message triggers clear cache in every validation cycle.
    if (validationNotFailedSoFar) {
      validationNotFailedSoFar = false;
      clearCache(messageWithInfo.getMessageInfo());
      boolean isValidMessage = apply(messageWithInfo);
      if (isValidMessage) {
        clearLoaderCache(messageWithInfo.getMessageInfo());
        validationNotFailedSoFar = true;
      }
      return isValidMessage;
    }
    return false;
  }

  private boolean validateMessageTypeConfigurations(IoTCoreMessageInfo messageInfo) {
    try {
      SchemaMapLoader.getSchemaMap(messageInfo);
      TableSchemaLoader.getSchema(messageInfo);
      TableDestinationLoader.getDestination(messageInfo);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private void clearCache(IoTCoreMessageInfo messageInfo) {
    final String cacheKey = GCPIoTCoreUtil.getDeviceCacheKey(messageInfo);
    GCPIoTCoreUtil.clearDeviceMetaDataCache(messageInfo);
    schemaCache.remove(cacheKey);
  }

  private void clearLoaderCache(IoTCoreMessageInfo messageInfo) {
    SchemaMapLoader.clearCache(messageInfo);
    TableDestinationLoader.clearCache(messageInfo);
  }

  private Map<String, Schema> getSchemaList(PubSubMessageWithMessageInfo message) {
    final String cacheKey = GCPIoTCoreUtil.getDeviceCacheKey(message.getMessageInfo());

    if (schemaCache.containsKey(cacheKey)) {
      return schemaCache.get(cacheKey);
    }

    String schemaString =
        GCPIoTCoreUtil.getMetaDataEntry(message.getMessageInfo(), INPUT_DATA_SCHEMA_META_DATA_KEY);
    if (schemaString == null) { // not schema available to validate against
      schemaCache.put(cacheKey, null);
      return null;
    }

    Map<String, Schema> schemaMap = new HashMap<>();

    JsonArray jsonSchemas = new JsonParser().parse(schemaString).getAsJsonArray();
    jsonSchemas.forEach(
        jsonSchema -> {
          JsonObject schemaObject = jsonSchema.getAsJsonObject();
          JSONObject schemaJsonObject =
              new JSONObject(
                  new JSONTokener(
                      new ByteArrayInputStream(
                          schemaObject.get(SCHEMA_FIELD).toString().getBytes())));
          Schema schema = SchemaLoader.load(schemaJsonObject);
          String dataType = schemaObject.get(DATA_TYPE_FIELD).getAsString();
          schemaMap.put(dataType, schema);
        });

    schemaCache.put(cacheKey, schemaMap);
    return schemaCache.get(cacheKey);
  }
}
