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

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionManager;
import reactor.core.publisher.Mono;

/**
 * A client wrapper around the Spanner {@link DatabaseClient}.
 */
public class SpannerClient implements Client {

  private final DatabaseClient databaseClient;

  public SpannerClient(DatabaseClient databaseClient) {
    this.databaseClient = databaseClient;
  }

  @Override
  public Mono<SpannerTransaction> startTransaction() {
    return Mono.fromSupplier(() -> {
      TransactionManager transactionManager = databaseClient.transactionManager();
      TransactionContext transactionContext = transactionManager.begin();
      return new SpannerTransaction(transactionManager, transactionContext);
    });
  }
}
