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

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.solutions.common.IoTCoreMessageInfo;
import com.google.cloud.solutions.common.TableRowWithMessageInfo;
import com.google.cloud.solutions.utils.GCPIoTCoreUtil;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class DynamicMessageTableRowMapperTest {

  @Test
  public void testTransform() throws IOException {
    DynamicMessageTableRowMapper cut = new DynamicMessageTableRowMapper();
    try (MockedStatic<GCPIoTCoreUtil> schemaLoaderMock = Mockito.mockStatic(GCPIoTCoreUtil.class)) {
      TableRowWithMessageInfo input = generateInput();
      schemaLoaderMock
          .when(
              () ->
                  GCPIoTCoreUtil.getMetaDataEntry(
                      input.getMessageInfo(),
                      "table-schema-" + input.getMessageInfo().getMessageType()))
          .thenReturn(loadTableSchema());
      TableRow tableRow = cut.apply(input);
      assertEquals("9876543210", tableRow.get("DeviceNumId"));
      assertEquals("test-reg", tableRow.get("RegistryId"));
      assertEquals("2021-01-02T03:04:05.000Z", tableRow.get("TimeStamp"));
      assertEquals("test-metric-type", tableRow.get("MetricType"));
      assertEquals("test-device", tableRow.get("Device"));
      assertEquals(42.43, tableRow.get("Value"));
      assertEquals("number", tableRow.get("ValueType"));
    }
  }

  private TableRowWithMessageInfo generateInput() {
    return new TableRowWithMessageInfo(generateMessageInfo(), generateTableRowMap());
  }

  private Map<String, String> generateTableRowMap() {
    Map<String, String> tableRowMap = new HashMap<>();
    tableRowMap.put("DeviceNumId", "9876543210");
    tableRowMap.put("RegistryId", "test-reg");
    tableRowMap.put("TimeStamp", "16095566450000");
    tableRowMap.put("MetricType", "test-metric-type");
    tableRowMap.put("Device", "test-device");
    tableRowMap.put("Value", "42.43");
    tableRowMap.put("ValueType", "number");
    return tableRowMap;
  }

  private String loadTableSchema() throws IOException {
    String path = "data-configs/edgex-table-schema.json";
    return readFile(path);
  }

  static String readFile(String path) throws IOException {
    byte[] encoded = Files.readAllBytes(Paths.get(path));
    return new String(encoded, StandardCharsets.UTF_8);
  }

  private IoTCoreMessageInfo generateMessageInfo() {
    IoTCoreMessageInfo messageInfo = new IoTCoreMessageInfo();
    messageInfo.setProjectId("test-project");
    messageInfo.setDeviceRegistryLocation("europe-west1");
    messageInfo.setDeviceRegistryId("test-reg");
    messageInfo.setDeviceId("test-device");
    messageInfo.setMessageType("test-type");
    return messageInfo;
  }
}
