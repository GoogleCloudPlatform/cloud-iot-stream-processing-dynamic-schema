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

import com.google.cloud.solutions.common.PubSubMessageWithMessageInfo;
import com.google.cloud.solutions.common.UnParsedMessage;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Transforms {@link PubSubMessageWithMessageInfo} to {@link UnParsedMessage} by extract the message
 * payload as string
 */
public class PubSubMessageToUnParsedMessage
    extends DoFn<PubSubMessageWithMessageInfo, UnParsedMessage> {

  private static final long serialVersionUID = 1L;

  @ProcessElement
  public void processElement(
      @Element PubSubMessageWithMessageInfo message, OutputReceiver<UnParsedMessage> receiver) {
    UnParsedMessage unParsedMessage = new UnParsedMessage();
    unParsedMessage.setMessageInfo(message.getMessageInfo());
    unParsedMessage.setMessage(new String(message.getPayload()));
    receiver.output(unParsedMessage);
  }
}
