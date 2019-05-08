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

package com.google.cloud.spanner.r2dbc;

import com.google.cloud.spanner.r2dbc.util.Assert;
import com.google.protobuf.Value;
import com.google.spanner.v1.ResultSetMetadata;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import java.util.List;
import java.util.function.BiFunction;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * {@link Result} implementation for Cloud Spanner.
 *
 */
public class SpannerResult implements Result {

  private final Flux<List<Value>> resultRows;

  private final ResultSetMetadata rowMetadata;

  private final Mono<Integer> rowsUpdated;

  /**
   * Constructor for read-only query execution.
   *
   * @param resultRows the underlying result from Cloud Spanner.
   */
  public SpannerResult(Flux<List<Value>> resultRows, ResultSetMetadata rowMetadata) {
    this.resultRows = Assert.requireNonNull(resultRows, "A non-null flux of rows is required.");
    this.rowMetadata = Assert.requireNonNull(rowMetadata, "Non-null row metadata is required.");
    this.rowsUpdated = Mono.just(0);
  }

  /**
   * Constructor for DML query execution.
   *
   * @param rowsUpdated the number of rows affected by the operation.
   */
  public SpannerResult(int rowsUpdated) {
    this.resultRows = null;
    this.rowMetadata = null;
    this.rowsUpdated = Mono.just(rowsUpdated);
  }

  @Override
  public Publisher<Integer> getRowsUpdated() {
    return this.rowsUpdated;
  }

  @Override
  public <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> f) {

    if (this.resultRows == null) {
      return Flux.empty();
    }

    return this.resultRows.map(row -> f
        .apply(new SpannerRow(row, this.rowMetadata), new SpannerRowMetadata(this.rowMetadata)));
  }
}
