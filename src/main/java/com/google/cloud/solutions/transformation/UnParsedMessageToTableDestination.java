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

import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.solutions.common.IoTCoreMessageInfo;
import com.google.cloud.solutions.common.UnParsedMessage;
import com.google.cloud.solutions.utils.TableDestinationLoader;
import com.google.cloud.solutions.utils.TableSchemaLoader;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.values.ValueInSingleWindow;

/**
 * Determines the BigQuery destination for unknown messages. Name of the dataset and the table are
 * read from configuration
 */
public class UnParsedMessageToTableDestination
    extends DynamicDestinations<UnParsedMessage, IoTCoreMessageInfo> {

  private static final long serialVersionUID = 1L;
  private static final String MESSAGE_TYPE = "unknown-message";

  @Override
  public IoTCoreMessageInfo getDestination(ValueInSingleWindow<UnParsedMessage> element) {
    return element.getValue().getMessageInfo();
  }

  @Override
  public TableDestination getTable(IoTCoreMessageInfo messageInfo) {
    messageInfo.setMessageType(MESSAGE_TYPE);
    return TableDestinationLoader.getDestination(messageInfo);
  }

  @Override
  public TableSchema getSchema(IoTCoreMessageInfo messageInfo) {
    messageInfo.setMessageType(MESSAGE_TYPE);
    return TableSchemaLoader.getSchema(messageInfo);
  }
}
