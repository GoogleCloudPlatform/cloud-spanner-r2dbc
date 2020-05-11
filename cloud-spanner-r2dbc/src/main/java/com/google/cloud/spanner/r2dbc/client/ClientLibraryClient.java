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

package com.google.cloud.spanner.r2dbc.client;

import com.google.cloud.spanner.r2dbc.StatementExecutionContext;
import com.google.longrunning.Operation;
import com.google.protobuf.Struct;
import com.google.spanner.v1.CommitResponse;
import com.google.spanner.v1.ExecuteBatchDmlRequest.Statement;
import com.google.spanner.v1.PartialResultSet;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.Session;
import com.google.spanner.v1.Transaction;
import com.google.spanner.v1.TransactionOptions;
import com.google.spanner.v1.Type;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Client-library-based {@link Client} implementation.
 */
public class ClientLibraryClient implements Client {

  @Override
  public Mono<Session> createSession(String databaseName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Mono<Void> deleteSession(String sessionName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Mono<Transaction> beginTransaction(String sessionName,
      TransactionOptions transactionOptions) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Mono<CommitResponse> commitTransaction(String sessionName, Transaction transaction) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Mono<Void> rollbackTransaction(String sessionName, Transaction transaction) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Flux<PartialResultSet> executeStreamingSql(StatementExecutionContext ctx, String sql,
      Struct params, Map<String, Type> types) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Flux<PartialResultSet> executeStreamingSql(StatementExecutionContext ctx, String sql) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Flux<ResultSet> executeBatchDml(StatementExecutionContext ctx,
      List<Statement> statements) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Mono<Operation> executeDdl(String fullyQualifiedDatabaseName, List<String> ddlStatement,
      Duration ddlOperationTimeout, Duration ddlPollInterval) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Mono<Void> close() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Mono<Boolean> healthcheck(StatementExecutionContext ctx) {
    throw new UnsupportedOperationException();
  }
}
