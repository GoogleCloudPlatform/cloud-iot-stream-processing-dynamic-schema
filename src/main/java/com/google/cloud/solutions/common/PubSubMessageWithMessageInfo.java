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

import com.google.cloud.solutions.utils.PubSubMessageUtil;
import java.io.Serializable;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

/**
 * POJO holder of raw data from a PubSub message together with the parsed message attributes in
 * {@link IoTCoreMessageInfo}
 */
public class PubSubMessageWithMessageInfo implements Serializable {
  private static final long serialVersionUID = -6308181018759264260L;
  private final IoTCoreMessageInfo messageInfo;
  private final byte[] payload;

  public PubSubMessageWithMessageInfo(final PubsubMessage pubsubMessage) {
    this.messageInfo = PubSubMessageUtil.extractIoTCoreMessageInfo(pubsubMessage);
    this.payload = pubsubMessage.getPayload();
  }

  public byte[] getPayload() {
    return payload;
  }

  public IoTCoreMessageInfo getMessageInfo() {
    return messageInfo;
  }
}
