/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.solutions.transformation;

import static org.junit.Assert.assertEquals;

import com.google.cloud.solutions.common.IoTCoreMessageInfo;
import com.google.cloud.solutions.common.PubSubMessageWithMessageInfo;
import com.google.cloud.solutions.common.TableRowWithMessageInfo;
import com.google.cloud.solutions.utils.GCPIoTCoreUtil;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.joda.time.Instant;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class PubSubMessageToTableRowMapperTest {

  @Test
  public void iotCoreAttributeMapping() throws InterruptedException {
    PubSubMessageToTableRowMapper cut = new PubSubMessageToTableRowMapper();
    String messageId = UUID.randomUUID().toString();
    PubsubMessage pubsubMessage =
        new PubsubMessage("{}".getBytes(), generateAttributeMap(), messageId);

    PubSubMessageWithMessageInfo message = new PubSubMessageWithMessageInfo(pubsubMessage);
    message.getMessageInfo().setMessageType("test-data-type");

    try (MockedStatic<GCPIoTCoreUtil> schemaMapLoaderMock =
        Mockito.mockStatic(GCPIoTCoreUtil.class)) {
      schemaMapLoaderMock
          .when(
              () ->
                  GCPIoTCoreUtil.getMetaDataEntry(
                      message.getMessageInfo(), "schema-map-test-data-type"))
          .thenReturn(generateIoTCoreAttributeMap());
      schemaMapLoaderMock
          .when(() -> GCPIoTCoreUtil.getDeviceCacheKeyWithMessageType(message.getMessageInfo()))
          .thenReturn(getDeviceCacheKeyWithMessageType(message.getMessageInfo()));
      OutputReceiver<TableRowWithMessageInfo> receiver = new TestReceiver();
      cut.processElement(message, receiver);
      List<TableRowWithMessageInfo> outputList = ((TestReceiver) receiver).getOutput();
      assertEquals(1, outputList.size());
      TableRowWithMessageInfo output = outputList.get(0);
      assertEquals("europe-west1", output.getTableRowMap().get("test_region_field"));
      assertEquals("test-reg", output.getTableRowMap().get("test_registry_field"));
      assertEquals("test-device", output.getTableRowMap().get("test_device_field"));
      assertEquals("test-project", output.getTableRowMap().get("test_project_field"));
      assertEquals("17131175321", output.getTableRowMap().get("test_deviceNo_field"));
      assertEquals(messageId, output.getTableRowMap().get("test_messageId_field"));
      assertEquals("testSub", output.getTableRowMap().get("test_subfolder_field"));
    }
  }

  @Test
  public void nestedArrayFieldMapping() throws InterruptedException {
    PubSubMessageToTableRowMapper cut = new PubSubMessageToTableRowMapper();
    PubsubMessage pubsubMessage =
        new PubsubMessage(generateNestedPayload().getBytes(), generateAttributeMap());
    PubSubMessageWithMessageInfo message = new PubSubMessageWithMessageInfo(pubsubMessage);
    message.getMessageInfo().setMessageType("test-nest-type");
    try (MockedStatic<GCPIoTCoreUtil> schemaMapLoaderMock =
        Mockito.mockStatic(GCPIoTCoreUtil.class)) {
      schemaMapLoaderMock
          .when(
              () ->
                  GCPIoTCoreUtil.getMetaDataEntry(
                      message.getMessageInfo(), "schema-map-test-nest-type"))
          .thenReturn(generateNestedFieldMap());
      schemaMapLoaderMock
          .when(() -> GCPIoTCoreUtil.getDeviceCacheKeyWithMessageType(message.getMessageInfo()))
          .thenReturn(getDeviceCacheKeyWithMessageType(message.getMessageInfo()));
      OutputReceiver<TableRowWithMessageInfo> receiver = new TestReceiver();
      cut.processElement(message, receiver);
      List<TableRowWithMessageInfo> outputList = ((TestReceiver) receiver).getOutput();
      assertEquals(2, outputList.size());
      TableRowWithMessageInfo output = outputList.get(0);
      assertEquals("1st-level-value1", output.getTableRowMap().get("Level_One_Key_One"));
      assertEquals("1st-level-value2", output.getTableRowMap().get("Level_One_Key_Two"));
      assertEquals("2nd-level-subval1", output.getTableRowMap().get("Level_Two_Key_One"));
      output = outputList.get(1);
      assertEquals("1st-level-value1", output.getTableRowMap().get("Level_One_Key_One"));
      assertEquals("1st-level-value2", output.getTableRowMap().get("Level_One_Key_Two"));
      assertEquals("2nd-level-subval2", output.getTableRowMap().get("Level_Two_Key_One"));
    }
  }

  private static Map<String, String> generateAttributeMap() {
    Map<String, String> attributeMap = new HashMap<>();
    attributeMap.put("deviceRegistryLocation", "europe-west1");
    attributeMap.put("deviceRegistryId", "test-reg");
    attributeMap.put("deviceId", "test-device");
    attributeMap.put("projectId", "test-project");
    attributeMap.put("deviceNumId", "17131175321");
    attributeMap.put("subFolder", "testSub");
    return attributeMap;
  }

  private static String generateNestedPayload() {
    return new JSONObject()
        .put("1st-level-key1", "1st-level-value1")
        .put("1st-level-key2", "1st-level-value2")
        .put(
            "1st-level-array",
            new JSONArray()
                .put(new JSONObject().put("2nd-level-key1", "2nd-level-subval1"))
                .put(new JSONObject().put("2nd-level-key1", "2nd-level-subval2")))
        .toString();
  }

  private static String generateIoTCoreAttributeMap() {
    return new JSONObject()
        .put("cloudIoT.attr.deviceRegistryLocation", "test_region_field")
        .put("cloudIoT.attr.deviceRegistryId", "test_registry_field")
        .put("cloudIoT.attr.deviceId", "test_device_field")
        .put("cloudIoT.attr.projectId", "test_project_field")
        .put("cloudIoT.attr.deviceNumId", "test_deviceNo_field")
        .put("cloudIoT.attr.subFolder", "test_subfolder_field")
        .put("cloudIoT.attr.messageId", "test_messageId_field")
        .toString();
  }

  private static String generateNestedFieldMap() {
    return new JSONObject()
        .put("1st-level-key1", "Level_One_Key_One")
        .put("1st-level-key2", "Level_One_Key_Two")
        .put("1st-level-array[]", new JSONObject().put("2nd-level-key1", "Level_Two_Key_One"))
        .toString();
  }

  public static class TestReceiver implements OutputReceiver<TableRowWithMessageInfo> {

    private List<TableRowWithMessageInfo> output = new ArrayList<>();

    @Override
    public void output(TableRowWithMessageInfo output) {
      this.output.add(output);
    }

    @Override
    public void outputWithTimestamp(TableRowWithMessageInfo output, Instant timestamp) {}

    public List<TableRowWithMessageInfo> getOutput() {
      return output;
    }
  }

  private static String getDeviceCacheKeyWithMessageType(IoTCoreMessageInfo messageInfo) {
    return String.format(
        "projects/%s/locations/%s/registries/%s/devices/%s/%s",
        messageInfo.getProjectId(),
        messageInfo.getDeviceRegistryLocation(),
        messageInfo.getDeviceRegistryId(),
        messageInfo.getDeviceId(),
        messageInfo.getMessageType());
  }
}
