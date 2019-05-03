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

package com.google.cloud.spanner.r2dbc.client;

import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionManager;
import reactor.core.publisher.Mono;

/**
 * Represents a transaction in Spanner.
 */
public final class SpannerTransaction {

  private final TransactionManager transactionManager;
  private final TransactionContext transactionContext;

  public SpannerTransaction(
      TransactionManager transactionManager, TransactionContext transactionContext) {
    this.transactionManager = transactionManager;
    this.transactionContext = transactionContext;
  }

  public Mono<TransactionManager.TransactionState> getTransactionState() {
    return Mono.fromSupplier(() -> transactionManager.getState());
  }

  public Mono<Void> rollback() {
    return Mono.fromRunnable(() -> transactionManager.rollback());
  }

  public Mono<Void> close() {
    return Mono.fromRunnable(() -> transactionManager.close());
  }

  public Mono<Void> commit() {
    return Mono.fromRunnable(() -> transactionManager.commit());
  }
}
