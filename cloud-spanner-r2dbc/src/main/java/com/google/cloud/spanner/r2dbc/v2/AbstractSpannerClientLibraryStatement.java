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

import com.google.cloud.spanner.r2dbc.statement.TypedNull;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import java.util.ArrayList;
import java.util.List;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cloud Spanner base implementation of R2DBC SPI for query and DML statements.
 *
 * <p>Supports parameter binding.
 */
abstract class AbstractSpannerClientLibraryStatement implements Statement {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractSpannerClientLibraryStatement.class);

  protected final DatabaseClientReactiveAdapter clientLibraryAdapter;

  protected final com.google.cloud.spanner.Statement.Builder currentStatementBuilder;

  private List<com.google.cloud.spanner.Statement> statements;


  /**
   * Creates a ready-to-run Cloud Spanner statement.
   * @param clientLibraryAdapter client library implementation of core functionality
   * @param query query to run, with `@` placeholders expected as parameters.
   */
  public AbstractSpannerClientLibraryStatement(
      DatabaseClientReactiveAdapter clientLibraryAdapter, String query) {
    this.clientLibraryAdapter = clientLibraryAdapter;
    this.currentStatementBuilder = com.google.cloud.spanner.Statement.newBuilder(query);
  }

  @Override
  public Statement add() {
    if (this.statements == null) {
      this.statements = new ArrayList<>();
      this.statements.add(this.currentStatementBuilder.build());
    }
    return this;
  }

  @Override
  public Publisher<? extends Result> execute() {
    return executeInternal();
  }

  public abstract Publisher<? extends Result> executeInternal();

  @Override
  public Statement bind(int index, Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Statement bind(String name, Object value) {
    ClientLibraryBinder.bind(this.currentStatementBuilder, name, value);
    return this;
  }

  @Override
  public Statement bindNull(int index, Class<?> type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Statement bindNull(String name, Class<?> type) {
    ClientLibraryBinder.bind(this.currentStatementBuilder, name, new TypedNull(type));
    return this;
  }

}
