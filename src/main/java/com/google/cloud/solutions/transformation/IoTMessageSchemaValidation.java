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
import com.google.cloud.solutions.utils.InputDataSchemaValidator;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Validate each {@link PubsubMessage} using {@link InputDataSchemaValidator} and branches the input
 * message stream into - known message stream, with positively validated messages - unknown message
 * stream, with negatively validated messages
 */
public class IoTMessageSchemaValidation extends DoFn<PubsubMessage, PubSubMessageWithMessageInfo> {

  private static final long serialVersionUID = 1L;
  private final TupleTag<PubSubMessageWithMessageInfo> knownMessageTag;
  private final TupleTag<PubSubMessageWithMessageInfo> unknownMessageTag;
  private final InputDataSchemaValidator inputDataSchemaValidator;

  public IoTMessageSchemaValidation(
      TupleTag<PubSubMessageWithMessageInfo> knownMessageTag,
      TupleTag<PubSubMessageWithMessageInfo> unknownMessageTag) {
    this.knownMessageTag = knownMessageTag;
    this.unknownMessageTag = unknownMessageTag;
    this.inputDataSchemaValidator = new InputDataSchemaValidator();
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    PubSubMessageWithMessageInfo messageWithInfo =
        new PubSubMessageWithMessageInfo(context.element());

    if (inputDataSchemaValidator.apply(messageWithInfo)) {
      context.output(knownMessageTag, messageWithInfo);
    } else {
      context.output(unknownMessageTag, messageWithInfo);
    }
  }
}
