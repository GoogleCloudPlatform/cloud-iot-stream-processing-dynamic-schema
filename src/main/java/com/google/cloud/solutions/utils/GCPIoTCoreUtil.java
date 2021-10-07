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

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudiot.v1.CloudIot;
import com.google.api.services.cloudiot.v1.CloudIotScopes;
import com.google.api.services.cloudiot.v1.model.Device;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.solutions.common.IoTCoreMessageInfo;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class from getting configurations stored as metadata in Cloud IoT Core. Setting up the
 * service object that calls Cloud IoT Core api. Returns the configuration value based on metadata
 * key. Previous fetched metadata are cached
 */
public class GCPIoTCoreUtil {
  private static final String APP_NAME = "dynamic-stream-processor";

  private static CloudIot service;

  private static final Map<String, Map<String, String>> metadataCache = new HashMap<>();

  public static CloudIot getService() {
    if (service == null) {
      try {
        initializeService();
      } catch (Exception e) {
        throw new Error("could not initialize IoT Core service", e);
      }
    }
    return service;
  }

  private static void initializeService() throws GeneralSecurityException, IOException {
    GoogleCredentials credential = createCredential();
    JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
    HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(credential);
    service =
        new CloudIot.Builder(
                GoogleNetHttpTransport.newTrustedTransport(), jsonFactory, requestInitializer)
            .setApplicationName(APP_NAME)
            .build();
  }

  private static GoogleCredentials createCredential() throws IOException {
    GoogleCredentials credential =
        GoogleCredentials.getApplicationDefault().createScoped(CloudIotScopes.all());
    return credential.createScoped(CloudIotScopes.all());
  }

  public static String getMetaDataEntry(IoTCoreMessageInfo messageInfo, String entryKey) {
    try {
      return getDeviceMetadata(messageInfo).get(entryKey);
    } catch (Exception e) {
      return null;
    }
  }

  public static String getDeviceCacheKey(IoTCoreMessageInfo messageInfo) {
    return String.format(
        "projects/%s/locations/%s/registries/%s/devices/%s",
        messageInfo.getProjectId(),
        messageInfo.getDeviceRegistryLocation(),
        messageInfo.getDeviceRegistryId(),
        messageInfo.getDeviceId());
  }

  public static String getDeviceCacheKeyWithMessageType(IoTCoreMessageInfo messageInfo) {
    return String.format(
        "projects/%s/locations/%s/registries/%s/devices/%s/%s",
        messageInfo.getProjectId(),
        messageInfo.getDeviceRegistryLocation(),
        messageInfo.getDeviceRegistryId(),
        messageInfo.getDeviceId(),
        messageInfo.getMessageType());
  }

  public static void clearDeviceMetaDataCache(IoTCoreMessageInfo messageInfo) {
    final String cacheKey = getDeviceCacheKey(messageInfo);
    metadataCache.remove(cacheKey);
  }

  private static Map<String, String> getDeviceMetadata(IoTCoreMessageInfo messageInfo)
      throws IOException {
    return getDeviceMetadata(
        messageInfo.getDeviceId(),
        messageInfo.getProjectId(),
        messageInfo.getDeviceRegistryLocation(),
        messageInfo.getDeviceRegistryId());
  }

  private static Map<String, String> getDeviceMetadata(
      String deviceId, String projectId, String cloudRegion, String registryName)
      throws IOException {
    final String devicePath =
        String.format(
            "projects/%s/locations/%s/registries/%s/devices/%s",
            projectId, cloudRegion, registryName, deviceId);
    if (metadataCache.containsKey(devicePath)) {
      return metadataCache.get(devicePath);
    }
    Device device = getDevice(devicePath);
    metadataCache.put(devicePath, ImmutableMap.copyOf(device.getMetadata()));
    return metadataCache.get(devicePath);
  }

  public static Device getDevice(String devicePath) throws IOException {
    return getService().projects().locations().registries().devices().get(devicePath).execute();
  }
}
