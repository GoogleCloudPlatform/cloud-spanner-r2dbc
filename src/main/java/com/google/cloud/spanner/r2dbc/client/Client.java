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

import com.google.cloud.spanner.r2dbc.ExecutionContext;
import com.google.protobuf.Struct;
import com.google.spanner.v1.CommitResponse;
import com.google.spanner.v1.ExecuteBatchDmlResponse;
import com.google.spanner.v1.PartialResultSet;
import com.google.spanner.v1.Session;
import com.google.spanner.v1.Transaction;
import com.google.spanner.v1.Type;
import java.util.List;
import java.util.Map;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * An abstraction that wraps interaction with the Cloud Spanner Database APIs.
 */
public interface Client {
  /**
   * Create a Spanner session to be used in subsequent interactions with the database.
   * @param databaseName Fully qualified Spanner database name in the format
   * {@code projects/[PROJECT_ID]/instances/[INSTANCE]/databases/[DATABASE]}
   * @returns {@link Mono} of the generated session.
   */
  Mono<Session> createSession(String databaseName);

  /**
   * Deletes a Spanner session that is used to call Spanner APIs.
   * @param ctx Execution context containing the current session.
   * @return {@link Mono} indicating completion closing the session.
   */
  Mono<Void> deleteSession(ExecutionContext ctx);

  /**
   * Begins a new Spanner {@link Transaction} within the provided {@link Session}.
   * @param ctx Execution context containing the current session.
   * @returns {@link Mono} of the transaction that was started.
   */
  Mono<Transaction> beginTransaction(ExecutionContext ctx);

  /**
   * Commits a Spanner {@link Transaction} within the provided {@link Session}.
   * @param ctx Execution context containing the current session and transaction.
   * @returns {@link CommitResponse} describing the timestamp at which the transaction committed.
   */
  Mono<CommitResponse> commitTransaction(ExecutionContext ctx);


  /**
   * Performs a rollback on a Spanner {@link Transaction} within the provided {@link Session}.
   * @param ctx Execution context containing the current session and transaction.
   * @return {@link Mono} indicating completion of the rollback.
   */
  Mono<Void> rollbackTransaction(ExecutionContext ctx);

  /**
   * Execute a streaming query and get partial results.
   * @param ctx Execution context containing the current session and optional transaction.
   */
  Flux<PartialResultSet> executeStreamingSql(ExecutionContext ctx, String sql, Struct params,
      Map<String, Type> types);

  default Flux<PartialResultSet> executeStreamingSql(ExecutionContext context, String sql) {
    return  executeStreamingSql(context, sql, null, null);
  }

  /**
   * Execute DML batch.
   */
  Mono<ExecuteBatchDmlResponse> executeBatchDml(ExecutionContext ctx, String sql,
      List<Struct> params, Map<String, Type> types);

  /**
   * Release any resources held by the {@link Client}.
   *
   * @return a {@link Mono} that indicates that a client has been closed
   */
  Mono<Void> close();
}
