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

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.r2dbc.util.Assert;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import java.util.function.BiFunction;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * {@link Result} implementation for Cloud Spanner.
 *
 */
public class SpannerResult implements Result {

  private final Flux<Struct> resultSet;

  private final Mono<Integer> rowsUpdated;

  /**
   * Constructor for read-only query execution.
   *
   * @param resultSet the underlying result from Cloud Spanner.
   */
  public SpannerResult(Flux<Struct> resultSet) {
    this.resultSet = Assert.requireNonNull(resultSet, "A non-null flux of rows is required.");
    this.rowsUpdated = Mono.just(0);
  }

  /**
   * Constructor for DML query execution.
   *
   * @param rowsUpdated the number of rows affected by the operation.
   */
  public SpannerResult(int rowsUpdated) {
    this.resultSet = null;
    this.rowsUpdated = Mono.just(rowsUpdated);
  }

  @Override
  public Publisher<Integer> getRowsUpdated() {
    return this.rowsUpdated;
  }

  @Override
  public <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> f) {

    if (this.resultSet == null) {
      return Flux.empty();
    }

    return this.resultSet.map(row -> f
        .apply(new SpannerRow(row), new SpannerRowMetadata(row)));
  }
}
