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

import com.google.cloud.spanner.r2dbc.client.Client;
import com.google.cloud.spanner.r2dbc.result.PartialResultRowExtractor;
import com.google.spanner.v1.PartialResultSet;
import com.google.spanner.v1.Session;
import com.google.spanner.v1.Transaction;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import javax.annotation.Nullable;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * {@link Statement} implementation for Cloud Spanner.
 */
public class SpannerStatement implements Statement {

  private Client client;

  private Session session;

  private Transaction transaction;

  private String sql;

  private int partialResultSetPrefetch = 1;

  /**
   * Creates a Spanner statement for a given SQL statement.
   *
   * <p>If no transaction is present, a temporary strongly consistent readonly transaction will be
   * used.
   *
   * @param client cloud spanner client to use for performing the query operation
   * @param session current cloud spanner session
   * @param transaction current cloud spanner transaction, or empty if no transaction is started
   * @param sql the query to execute
   */
  public SpannerStatement(
      Client client,
      Session session,
      @Nullable Transaction transaction,
      String sql) {

    this.client = client;
    this.session = session;
    this.transaction = transaction;
    this.sql = sql;
  }

  @Override
  public Statement add() {
    return null;
  }

  @Override
  public Statement bind(Object o, Object o1) {
    return null;
  }

  @Override
  public Statement bind(int i, Object o) {
    return null;
  }

  @Override
  public Statement bindNull(Object o, Class<?> type) {
    return null;
  }

  @Override
  public Statement bindNull(int i, Class<?> type) {
    return null;
  }

  @Override
  public Publisher<? extends Result> execute() {
    PartialResultRowExtractor partialResultRowExtractor = new PartialResultRowExtractor();

    return this.client.executeStreamingSql(this.session, this.transaction, this.sql)
        .switchOnFirst((signal, flux) -> {
          if (signal.hasError()) {
            return Mono.error(signal.getThrowable());
          }
          if (signal.isOnComplete()) {
            // Empty response returned from Cloud Spanner.
            return flux.then(Mono.just(new SpannerResult(Flux.empty(), Mono.just(0))));
          }

          PartialResultSet firstPartialResultSet = signal.get();
          if (isDmlQuery(firstPartialResultSet)) {
            Mono<Integer> rowsChanged =
                flux.last()
                    .map(partialResultSet ->
                        Math.toIntExact(partialResultSet.getStats().getRowCountExact()));

            return Mono.just(new SpannerResult(Flux.empty(), rowsChanged));
          } else {
            return Mono.just(new SpannerResult(
                flux.flatMapIterable(partialResultRowExtractor, this.partialResultSetPrefetch),
                Mono.just(0)));
          }
        })
        .next();
  }

  /**
   * Returns whether we think the query is a DML query.
   */
  private static boolean isDmlQuery(PartialResultSet firstPartialResultSet) {
    return firstPartialResultSet.getMetadata().getRowType().getFieldsList().isEmpty();
  }

  /**
   * Allows customizing the number of {@link PartialResultSet} objects to request at a time.
   * @param prefetch batch size to request from Cloud Spanner
   */
  public void setPartialResultSetPrefetch(int prefetch) {
    this.partialResultSetPrefetch = prefetch;
  }

}
