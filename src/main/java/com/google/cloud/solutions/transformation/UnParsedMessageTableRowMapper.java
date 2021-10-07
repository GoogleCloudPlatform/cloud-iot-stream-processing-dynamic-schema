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

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.solutions.common.IoTCoreMessageInfo;
import com.google.cloud.solutions.common.UnParsedMessage;
import org.apache.beam.sdk.transforms.SerializableFunction;

/** Transform UnParsedMessage to destination table format */
public class UnParsedMessageTableRowMapper
    implements SerializableFunction<UnParsedMessage, TableRow> {

  private static final long serialVersionUID = 1L;

  @Override
  public TableRow apply(UnParsedMessage input) {
    TableRow tableRow = new TableRow();
    IoTCoreMessageInfo messageInfo = input.getMessageInfo();
    tableRow
        .set("DeviceNumId", messageInfo.getDeviceNumId())
        .set("DeviceId", messageInfo.getDeviceId())
        .set("RegistryId", messageInfo.getDeviceRegistryId())
        .set("Message", input.getMessage());
    return tableRow;
  }
}
