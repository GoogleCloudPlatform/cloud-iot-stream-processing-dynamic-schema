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
package com.google.cloud.solutions.common;

import java.io.Serializable;
import java.util.Map;

/**
 * POJO holder of a BigQuery table row in a string to string map together with the IoT Core message
 * attributes in {@link IoTCoreMessageInfo} The map key is the field name The map value is the
 * string representation of the field value
 */
public class TableRowWithMessageInfo implements Serializable {
  private static final long serialVersionUID = 5038761469411530015L;
  private final IoTCoreMessageInfo messageInfo;
  private final Map<String, String> tableRowMap;

  public TableRowWithMessageInfo(
      final IoTCoreMessageInfo messageInfo, final Map<String, String> tableRowMap) {
    this.messageInfo = messageInfo;
    this.tableRowMap = tableRowMap;
  }

  public IoTCoreMessageInfo getMessageInfo() {
    return messageInfo;
  }

  public Map<String, String> getTableRowMap() {
    return tableRowMap;
  }
}
