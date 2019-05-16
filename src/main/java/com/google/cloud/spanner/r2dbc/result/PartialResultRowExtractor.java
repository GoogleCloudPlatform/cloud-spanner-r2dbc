/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spanner.r2dbc.result;

import com.google.cloud.spanner.r2dbc.SpannerRow;
import com.google.cloud.spanner.r2dbc.SpannerRowMetadata;
import com.google.protobuf.Value;
import com.google.spanner.v1.PartialResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import reactor.core.publisher.Mono;

/**
 * // TODO: this should also track the latest resume_token and return it upon request
 */
public class PartialResultRowExtractor {

  AtomicReference<SpannerRowMetadata> metadata = new AtomicReference<>(null);

  public List<SpannerRow> extractCompleteRows(PartialResultSet partialResultSet) {
    List<SpannerRow> fullRows = new ArrayList<>();

    if (partialResultSet.hasMetadata()) {
      metadata.compareAndSet(null, new SpannerRowMetadata(partialResultSet.getMetadata()));
    }
    // TODO: handle the case where metdata is not available yet
    if (metadata.get() == null) {
      throw new RuntimeException("Metadata failed to arrive with the first PartialResultSet");
    }
    StructType rowType = metadata.get().getRowMetadata().getRowType();

    int fieldsPerRow = rowType.getFieldsCount();

    // TODO: handle partials left over at the end of previous row
    List<Value> values = partialResultSet.getValuesList();

    // TODO: account for chunked values (field split between partial result sets).
    int startIndex = 0;
    int endIndex = fieldsPerRow;

    // this loop takes care of multiple rows per PartialResultSet
    while (endIndex <= values.size()) {
      System.out.println("looking up columns from " + startIndex + " to " + endIndex);

      List<Value> singleRowValues = values.subList(startIndex, endIndex);
      System.out.println("A single row: " + singleRowValues);
      // TODO: add row metadata
      fullRows.add(new SpannerRow(singleRowValues,  metadata.get()));

      startIndex += fieldsPerRow;
      endIndex += fieldsPerRow;

    }

    return fullRows;
  }

}
