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

/**
 * POJO holder of the specific PubSub message attributes that are populated by Cloud IoT Core
 *
 * @see <a
 *     href="https://cloud.google.com/iot/docs/how-tos/mqtt-bridge#publishing_telemetry_events">Cloud
 *     IoT Core doc</a>
 */
public class IoTCoreMessageInfo implements Serializable {
  private static final long serialVersionUID = -6129094259910824985L;

  private String deviceNumId;
  private String deviceId;
  private String deviceRegistryId;
  private String deviceRegistryLocation;
  private String projectId;
  private String subFolder;
  private String messageType;
  private String messageId;

  public String getDeviceNumId() {
    return deviceNumId;
  }

  public void setDeviceNumId(String deviceNumId) {
    this.deviceNumId = deviceNumId;
  }

  public String getDeviceId() {
    return deviceId;
  }

  public void setDeviceId(String deviceId) {
    this.deviceId = deviceId;
  }

  public String getDeviceRegistryId() {
    return deviceRegistryId;
  }

  public void setDeviceRegistryId(String deviceRegistryId) {
    this.deviceRegistryId = deviceRegistryId;
  }

  public String getDeviceRegistryLocation() {
    return deviceRegistryLocation;
  }

  public void setDeviceRegistryLocation(String deviceRegistryLocation) {
    this.deviceRegistryLocation = deviceRegistryLocation;
  }

  public String getProjectId() {
    return projectId;
  }

  public void setProjectId(String projectId) {
    this.projectId = projectId;
  }

  public String getSubFolder() {
    return subFolder;
  }

  public void setSubFolder(String subFolder) {
    this.subFolder = subFolder;
  }

  public String getMessageType() {
    return messageType;
  }

  public void setMessageType(String messageType) {
    this.messageType = messageType;
  }

  public String getMessageId() {
    return messageId;
  }

  public void setMessageId(String messageId) {
    this.messageId = messageId;
  }
}
