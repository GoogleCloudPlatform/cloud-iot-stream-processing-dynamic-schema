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
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

/**
 * Extract the Cloud IoT Core message attributes from PubsubMessage and populate the in the
 * IoTCoreMessageInfo POJO
 */
public class PubSubMessageUtil {
  private static final String UNKNOWN_MESSAGE_TYPE = "unknown";

  public static IoTCoreMessageInfo extractIoTCoreMessageInfo(PubsubMessage message) {
    IoTCoreMessageInfo messageInfo = new IoTCoreMessageInfo();
    messageInfo.setMessageId(message.getMessageId());
    messageInfo.setDeviceNumId(message.getAttribute("deviceNumId"));
    messageInfo.setDeviceId(message.getAttribute("deviceId"));
    messageInfo.setDeviceRegistryId(message.getAttribute("deviceRegistryId"));
    messageInfo.setDeviceRegistryLocation(message.getAttribute("deviceRegistryLocation"));
    messageInfo.setProjectId(message.getAttribute("projectId"));
    messageInfo.setSubFolder(message.getAttribute("subFolder"));
    messageInfo.setMessageType(UNKNOWN_MESSAGE_TYPE);
    return messageInfo;
  }
}
