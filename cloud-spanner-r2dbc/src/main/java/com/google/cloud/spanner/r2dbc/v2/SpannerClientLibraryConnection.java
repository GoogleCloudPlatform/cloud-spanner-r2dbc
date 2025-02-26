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

import static com.google.cloud.spanner.r2dbc.v2.SpannerConstants.TIMESTAMP_BOUND;
import static com.google.common.base.MoreObjects.firstNonNull;
import static io.r2dbc.spi.IsolationLevel.SERIALIZABLE;
import static io.r2dbc.spi.TransactionDefinition.ISOLATION_LEVEL;
import static io.r2dbc.spi.TransactionDefinition.READ_ONLY;
import static java.lang.Boolean.TRUE;

import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spanner.r2dbc.SpannerConnectionMetadata;
import com.google.cloud.spanner.r2dbc.api.SpannerConnection;
import com.google.cloud.spanner.r2dbc.statement.StatementParser;
import com.google.cloud.spanner.r2dbc.statement.StatementType;
import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionMetadata;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.TransactionDefinition;
import io.r2dbc.spi.ValidationDepth;
import java.time.Duration;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

class SpannerClientLibraryConnection implements Connection, SpannerConnection {

  private final DatabaseClientReactiveAdapter clientLibraryAdapter;
  private final ConnectionMetadata metadata = SpannerConnectionMetadata.INSTANCE;

  /**
   * Cloud Spanner implementation of R2DBC Connection SPI.
   *
   * @param clientLibraryAdapter adapter to Cloud Spanner database client
   */
  public SpannerClientLibraryConnection(DatabaseClientReactiveAdapter clientLibraryAdapter) {
    this.clientLibraryAdapter = clientLibraryAdapter;
  }

  @Override
  public Publisher<Void> beginTransaction() {
    return this.clientLibraryAdapter.beginTransaction();
  }

  @Override
  public Publisher<Void> beginTransaction(TransactionDefinition definition) {
    IsolationLevel isolationLevel = firstNonNull(definition.getAttribute(ISOLATION_LEVEL),
        SERIALIZABLE);
    return validateIsolation(isolationLevel)
        .then(Mono.defer(() -> {
          boolean isReadOnly = TRUE.equals(definition.getAttribute(READ_ONLY));
          if (isReadOnly) {
            TimestampBound timestampBound = firstNonNull(definition.getAttribute(TIMESTAMP_BOUND),
                TimestampBound.strong());
            return this.clientLibraryAdapter.beginReadonlyTransaction(timestampBound);
          }
          return this.clientLibraryAdapter.beginTransaction();
        }));
  }

  @Override
  public Publisher<Void> setLockWaitTimeout(Duration timeout) {
    return Mono.error(new UnsupportedOperationException());
  }

  @Override
  public Publisher<Void> setStatementTimeout(Duration timeout) {
    return Mono.error(new UnsupportedOperationException());
  }

  @Override
  public Mono<Void> beginReadonlyTransaction(TimestampBound timestampBound) {
    return this.clientLibraryAdapter.beginReadonlyTransaction(timestampBound);
  }

  @Override
  public Mono<Void> beginReadonlyTransaction() {
    return this.clientLibraryAdapter.beginReadonlyTransaction(TimestampBound.strong());
  }

  @Override
  public Publisher<Void> commitTransaction() {
    return this.clientLibraryAdapter.commitTransaction();
  }

  @Override
  public Batch createBatch() {
    return new SpannerBatch(this.clientLibraryAdapter);
  }

  @Override
  public Publisher<Void> createSavepoint(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Statement createStatement(String query) {
    if (query == null) {
      throw new IllegalArgumentException("Invalid null query.");
    }
    StatementType type = StatementParser.getStatementType(query);
    if (type == StatementType.DDL) {
      return new SpannerClientLibraryDdlStatement(query, this.clientLibraryAdapter);
    } else if (type == StatementType.DML) {
      return new SpannerClientLibraryDmlStatement(this.clientLibraryAdapter, query);
    }
    return new SpannerClientLibraryStatement(this.clientLibraryAdapter, query);
  }

  @Override
  public boolean isAutoCommit() {
    return this.clientLibraryAdapter.isAutoCommit();
  }

  @Override
  public ConnectionMetadata getMetadata() {
    return this.metadata;
  }

  @Override
  public IsolationLevel getTransactionIsolationLevel() {
    return IsolationLevel.SERIALIZABLE;
  }

  @Override
  public Publisher<Void> releaseSavepoint(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Publisher<Void> rollbackTransaction() {
    return this.clientLibraryAdapter.rollback();
  }

  @Override
  public Publisher<Void> rollbackTransactionToSavepoint(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Publisher<Void> setAutoCommit(boolean autoCommit) {
    return this.clientLibraryAdapter.setAutoCommit(autoCommit);
  }


  @Override
  public Publisher<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
    return this.validateIsolation(isolationLevel);
  }

  @Override
  public Publisher<Boolean> validate(ValidationDepth depth) {
    if (depth == ValidationDepth.LOCAL) {
      return this.clientLibraryAdapter.localHealthcheck();
    } else {
      return this.clientLibraryAdapter.healthCheck();
    }
  }

  @Override
  public Publisher<Void> close() {
    return this.clientLibraryAdapter.close();
  }

  public boolean isInReadonlyTransaction() {
    return this.clientLibraryAdapter.isInReadonlyTransaction();
  }

  private Mono<Void> validateIsolation(IsolationLevel isolationLevel) {
    if (isolationLevel == null) {
      return Mono.error(new IllegalArgumentException("IsolationLevel can't be null."));
    }
    return isolationLevel == SERIALIZABLE ? Mono.empty()
        : Mono.error(new UnsupportedOperationException(
            String.format("Unsupported '%s' isolation level, Only SERIALIZABLE is supported.",
                isolationLevel.asSql())));
  }
}
