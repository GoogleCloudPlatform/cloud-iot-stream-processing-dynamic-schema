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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.solutions.common.TableRowWithMessageInfo;
import com.google.cloud.solutions.utils.TableSchemaLoader;
import java.util.Map;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Transforms the row map in {@link TableRowWithMessageInfo} to BigQuery {@link TableRow} object The
 * the BigQuery table schema is provided dynamically by {@link TableSchemaLoader} Following BigQuery
 * data types are supported:
 *
 * <p>
 *
 * <ul>
 *   <li>STRING
 *   <li>INT64
 *   <li>FLOAT
 *   <li>TIMESTAMP
 * </ul>
 *
 * <p>
 */
public class DynamicMessageTableRowMapper
    implements SerializableFunction<TableRowWithMessageInfo, TableRow> {

  private static final long serialVersionUID = 6891268007272455587L;
  private static DateTimeFormatter TIME_STAMP_FORMAT =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").withZone(DateTimeZone.forID("UTC"));

  @Override
  public TableRow apply(TableRowWithMessageInfo input) {
    Map<String, TableFieldSchema> tableSchema =
        TableSchemaLoader.getFieldMap(input.getMessageInfo());
    return toTableRow(input.getTableRowMap(), tableSchema);
  }

  private TableRow toTableRow(Map<String, String> row, Map<String, TableFieldSchema> tableSchema) {
    TableRow tableRow = new TableRow();
    for (String key : row.keySet()) {
      if (tableSchema.containsKey(key)) {
        TableFieldSchema tableFieldSchema = tableSchema.get(key);
        tableRow.put(key, createTableRowField(row.get(key), tableFieldSchema));
      }
    }
    return tableRow;
  }

  private Object createTableRowField(String valueStr, TableFieldSchema tableFieldSchema) {
    Object retObject = null;

    switch (tableFieldSchema.getType().toLowerCase()) {
      case "int64":
        retObject = Long.parseLong(valueStr);
        break;
      case "timestamp":
        retObject = parseTimeStamp(valueStr);
        break;
      case "float":
        retObject = Double.parseDouble(valueStr);
        break;
      case "string":
      default:
        retObject = valueStr;
        break;
    }
    return retObject;
  }

  private Object parseTimeStamp(String valueStr) {
    try {
      long epochMilli = Long.parseLong(valueStr);
      if (valueStr.length() > 13) {
        epochMilli = Long.parseLong(valueStr.substring(0, 13));
      }
      return Instant.ofEpochMilli(epochMilli).toString();
    } catch (NumberFormatException nfe) {
    }

    try {
      return Instant.parse(valueStr, TIME_STAMP_FORMAT).toString();
    } catch (Exception e) {
    }

    return valueStr;
  }
}
