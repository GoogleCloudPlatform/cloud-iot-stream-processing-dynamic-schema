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
package com.google.cloud.solutions.transformation;

import com.google.cloud.solutions.common.IoTCoreMessageInfo;
import com.google.cloud.solutions.common.PubSubMessageWithMessageInfo;
import com.google.cloud.solutions.common.TableRowWithMessageInfo;
import com.google.cloud.solutions.utils.SchemaMapLoader;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.lang.reflect.Method;
import java.util.*;
import java.util.Map.Entry;
import org.apache.beam.sdk.transforms.DoFn;

/** Transforms the JSON payload of PubSub message to {@link TableRowWithMessageInfo} */
public class PubSubMessageToTableRowMapper
    extends DoFn<PubSubMessageWithMessageInfo, TableRowWithMessageInfo> {
  private static final String IOT_CORE_ATTRIBUTE_PREFIX = "cloudIoT.attr.";

  @ProcessElement
  public void processElement(
      @Element PubSubMessageWithMessageInfo message,
      OutputReceiver<TableRowWithMessageInfo> receiver) {
    JsonObject payloadJson =
        new JsonParser().parse(new String(message.getPayload())).getAsJsonObject();
    JsonObject schemaMap = SchemaMapLoader.getSchemaMap(message.getMessageInfo());

    List<Map<String, String>> rows = new ArrayList<>();
    Map<String, String> rowValues = new HashMap<>();
    rows.add(rowValues);

    parsePayload(payloadJson, schemaMap, message.getMessageInfo(), rows);

    for (Map<String, String> row : rows) {
      receiver.output(new TableRowWithMessageInfo(message.getMessageInfo(), row));
    }
  }

  /**
   * @param payload Parsed JSON payload from PubSub message
   * @param mapper Field map configuration, where the key is the field identifier of the in payload
   *     and value represent the field identifier of the destination Table
   * @param messageInfo containing IoT Core message attribute and payload message type
   * @param rows holder for the extract key value mapping, where the key is the field identifier and
   *     value is extracted value from payload object For every key value pair in the mapper, use
   *     the key to extract value from messageInfo or payload and populate the value in the rows
   *     holder.
   */
  private void parsePayload(
      JsonObject payload,
      JsonObject mapper,
      IoTCoreMessageInfo messageInfo,
      List<Map<String, String>> rows) {
    for (Entry<String, JsonElement> entry : mapper.entrySet()) {
      if (entry.getKey().startsWith(IOT_CORE_ATTRIBUTE_PREFIX)) {
        String value =
            getIoTCoreAttributeField(
                messageInfo, entry.getKey().substring(IOT_CORE_ATTRIBUTE_PREFIX.length()));
        if (value != null) {
          for (Map<String, String> rowValues : rows) {
            rowValues.put(entry.getValue().getAsString(), value);
          }
        }
      } else {
        String[] keys = entry.getKey().split("\\.");
        storeFieldValue(payload, keys, entry.getValue(), messageInfo, rows);
      }
    }
  }

  /**
   * Traversing down the nested payload object following the path specified in the payloadKeyPath
   * array. Store the extracted value in rows mapper using destinationTableField as key
   */
  private void storeFieldValue(
      JsonObject payloadJson,
      String[] payloadKeyPath,
      JsonElement destinationTableField,
      IoTCoreMessageInfo messageInfo,
      List<Map<String, String>> rows) {
    if (payloadKeyPath.length == 1) {
      String currentPayloadKey = payloadKeyPath[0];
      if (currentPayloadKey.endsWith("[]")) {
        List<Map<String, String>> rowsHolder = new ArrayList<>();

        for (JsonElement jsonElement :
            payloadJson
                .get(currentPayloadKey.substring(0, currentPayloadKey.length() - 2))
                .getAsJsonArray()) {
          JsonObject payloadArrayItem = jsonElement.getAsJsonObject();

          for (Map<String, String> rowValues : rows) {
            Map<String, String> rowCopy = new HashMap<>(rowValues);
            List<Map<String, String>> rowsCopy = new ArrayList<>();
            rowsCopy.add(rowCopy);
            parsePayload(
                payloadArrayItem, destinationTableField.getAsJsonObject(), messageInfo, rowsCopy);
            rowsHolder.addAll(rowsCopy);
          }
        }
        rows.clear();
        rows.addAll(rowsHolder);
      } else {
        for (Map<String, String> rowValues : rows) {
          rowValues.put(
              destinationTableField.getAsString(),
              payloadJson.get(currentPayloadKey).getAsString());
        }
      }
    } else {
      storeFieldValue(
          payloadJson.getAsJsonObject(payloadKeyPath[0]),
          getSubKeys(payloadKeyPath),
          destinationTableField,
          messageInfo,
          rows);
    }
  }

  private String[] getSubKeys(String[] keys) {
    String[] subKeys = new String[keys.length - 1];
    System.arraycopy(keys, 1, subKeys, 0, subKeys.length);
    return subKeys;
  }

  private String getIoTCoreAttributeField(IoTCoreMessageInfo messageInfo, String fieldName) {
    try {
      Method fieldGetter =
          messageInfo
              .getClass()
              .getMethod("get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1));
      return fieldGetter.invoke(messageInfo).toString();
    } catch (Exception ignore) {
      return null;
    }
  }
}
