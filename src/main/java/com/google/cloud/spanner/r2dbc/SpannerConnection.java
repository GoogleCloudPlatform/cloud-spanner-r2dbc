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

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.TransactionManager;
import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Statement;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * {@link Connection} implementation for Cloud Spanner.
 */
public class SpannerConnection implements Connection {

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  private final DatabaseClient databaseClient;

  private SpannerTransaction currentTransaction;

  public SpannerConnection(DatabaseClient databaseClient) {
    this.databaseClient = databaseClient;
    this.currentTransaction = null;
  }

  @Override
  public Publisher<Void> beginTransaction() {
    return getTransactionState()
        .flatMap(transactionState -> {
          if (transactionState == TransactionManager.TransactionState.STARTED) {
            logger.debug(
                "A transaction has already been started for this connection; "
                    + "beginTransaction() is a no-op.");
            return Mono.just(currentTransaction);
          } else {
            return SpannerTransaction.startTransaction(databaseClient);
          }
        })
        .doOnSuccess(spannerTransaction -> {
          if (currentTransaction != null) {
            currentTransaction.close();
          }
          currentTransaction = spannerTransaction;
        }).then();
  }

  @Override
  public Publisher<Void> close() {
    return Mono.fromRunnable(() -> {
      if (currentTransaction != null) {
        currentTransaction.close();
      }
    });
  }

  @Override
  public Publisher<Void> commitTransaction() {
    return getTransactionState()
        .flatMap(transactionState -> {
          if (transactionState != TransactionManager.TransactionState.STARTED) {
            logger.debug(
                "Transaction is already in a terminal state; commitTransaction() is a no-op.");
            return Mono.empty();
          } else {
            return currentTransaction.commit();
          }
        });
  }

  @Override
  public Batch createBatch() {
    return null;
  }

  @Override
  public Publisher<Void> createSavepoint(String s) {
    return null;
  }

  @Override
  public Statement createStatement(String sql) {
    return null;
  }

  @Override
  public Publisher<Void> releaseSavepoint(String s) {
    return null;
  }

  @Override
  public Publisher<Void> rollbackTransaction() {
    return null;
  }

  @Override
  public Publisher<Void> rollbackTransactionToSavepoint(String s) {
    return null;
  }

  @Override
  public Publisher<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
    return null;
  }

  private Mono<TransactionManager.TransactionState> getTransactionState() {
    return Mono.defer(() -> {
      if (currentTransaction != null) {
        return currentTransaction.getTransactionState();
      } else {
        return Mono.just(TransactionManager.TransactionState.ABORTED);
      }
    });
  }
}
