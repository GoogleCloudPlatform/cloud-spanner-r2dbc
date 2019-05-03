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

import com.google.cloud.spanner.TransactionManager.TransactionState;
import com.google.cloud.spanner.r2dbc.client.Client;
import com.google.cloud.spanner.r2dbc.client.SpannerTransaction;
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

  private final Client client;

  private SpannerTransaction currentTransaction;

  public SpannerConnection(Client client) {
    this.client = client;
    this.currentTransaction = null;
  }

  @Override
  public Publisher<Void> beginTransaction() {
    return isInTransaction()
        .flatMap(isInTransaction -> {
          if (isInTransaction) {
            logger.debug(
                "A transaction has already been started for this connection; "
                    + "beginTransaction() is a no-op.");
            return Mono.empty();
          } else {
            return client.startTransaction()
                .doOnSuccess(newTransaction -> currentTransaction = newTransaction)
                .then();
          }
        })
        .then();
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
    return isInTransaction()
        .flatMap(isInTransaction -> {
          if (!isInTransaction) {
            logger.debug("Transaction is not active; commitTransaction() is a no-op.");
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

  private Mono<Boolean> isInTransaction() {
    return Mono.defer(() -> {
      if (currentTransaction != null) {
        return currentTransaction.getTransactionState()
            .map(transactionState -> transactionState == TransactionState.STARTED);
      } else {
        return Mono.just(false);
      }
    });
  }
}
