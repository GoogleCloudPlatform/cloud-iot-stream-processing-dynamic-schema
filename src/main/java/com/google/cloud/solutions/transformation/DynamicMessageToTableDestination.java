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
import com.google.cloud.solutions.common.TableRowWithMessageInfo;
import com.google.cloud.solutions.utils.TableDestinationLoader;
import com.google.cloud.solutions.utils.TableSchemaLoader;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.values.ValueInSingleWindow;

/**
 * Determines the BigQuery destination where the message is written to based on the determined
 * message type.
 */
public class DynamicMessageToTableDestination
    extends DynamicDestinations<TableRowWithMessageInfo, IoTCoreMessageInfo> {
  private static final long serialVersionUID = -1519270901335637794L;

  @Override
  public IoTCoreMessageInfo getDestination(ValueInSingleWindow<TableRowWithMessageInfo> element) {
    return element.getValue().getMessageInfo();
  }

  @Override
  public TableDestination getTable(IoTCoreMessageInfo messageInfo) {
    return TableDestinationLoader.getDestination(messageInfo);
  }

  @Override
  public TableSchema getSchema(IoTCoreMessageInfo messageInfo) {
    return TableSchemaLoader.getSchema(messageInfo);
  }
}
