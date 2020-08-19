/*
 * Copyright 2019-2020 Google LLC
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

package com.google.cloud.spanner.r2dbc.v2;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.r2dbc.v2.client.ClientLibraryReactiveAdapter;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Cloud Spanner implementation of R2DBC SPI for DML statements.
 */
public class SpannerClientLibraryDmlStatement implements Statement {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SpannerClientLibraryDmlStatement.class);
  
  // YOLO; very temporary. TODO: use global one in SpannerClientLibraryConnection.
  private ExecutorService executorService = Executors.newSingleThreadExecutor();

  private DatabaseClient databaseClient;

  private ClientLibraryReactiveAdapter clientLibraryAdapter;

  private String query;

  // TODO (elfel): adapter has client; remove client from here
  /**
   * Creates a ready-to-run Cloud Spanner DML statement.
   * @param databaseClient Cloud Spanner client library database client
   * @param clientLibraryAdapter REMOVE THIS
   * @param query query to run
   */
  // TODO: accept a transaction
  public SpannerClientLibraryDmlStatement(
      DatabaseClient databaseClient, ClientLibraryReactiveAdapter clientLibraryAdapter,
      String query) {
    this.databaseClient = databaseClient;
    this.clientLibraryAdapter = clientLibraryAdapter;
    this.query = query;
  }

  @Override
  public Statement add() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Statement bind(int index, Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Statement bind(String name, Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Statement bindNull(int index, Class<?> type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Statement bindNull(String name, Class<?> type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Publisher<? extends Result> execute() {
    return this.clientLibraryAdapter
        .runDmlStatement(com.google.cloud.spanner.Statement.of(this.query))
        .transform(numRowsUpdatedMono -> Mono.just(
            new SpannerClientLibraryResult(Flux.empty(), numRowsUpdatedMono.map(this::longToInt))));
  }

  private int longToInt(Long numRows) {
    if (numRows > Integer.MAX_VALUE) {
      LOGGER.warn("Number of updated rows exceeds maximum integer value; actual rows updated = %s; "
          + "returning max int value", numRows);
      return Integer.MAX_VALUE;
    }
    return numRows.intValue();
  }

}
