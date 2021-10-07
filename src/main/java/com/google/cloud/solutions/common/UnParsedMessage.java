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
 * POJO holder of PubSub message as string together with the parsed message attributes in {@link
 * IoTCoreMessageInfo}
 */
public class UnParsedMessage implements Serializable {
  private static final long serialVersionUID = 1L;
  private IoTCoreMessageInfo messageInfo;
  private String message;

  public IoTCoreMessageInfo getMessageInfo() {
    return messageInfo;
  }

  public void setMessageInfo(IoTCoreMessageInfo messageInfo) {
    this.messageInfo = messageInfo;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }
}
