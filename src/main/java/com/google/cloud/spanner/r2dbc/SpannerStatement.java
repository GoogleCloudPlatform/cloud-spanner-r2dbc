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

import static java.util.Objects.requireNonNull;

import com.google.cloud.spanner.r2dbc.client.Client;
import com.google.cloud.spanner.r2dbc.codecs.Codec;
import com.google.cloud.spanner.r2dbc.codecs.Codecs;
import com.google.cloud.spanner.r2dbc.codecs.DefaultCodecs;
import com.google.cloud.spanner.r2dbc.result.PartialResultRowExtractor;
import com.google.protobuf.Struct;
import com.google.protobuf.Struct.Builder;
import com.google.spanner.v1.PartialResultSet;
import com.google.spanner.v1.Session;
import com.google.spanner.v1.Type;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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

  private SpannerTransactionContext transaction;

  private String sql;

  private LinkedList<Map<String, Object>> bindings = new LinkedList<>();

  private Codecs codecs = new DefaultCodecs();

  private Map<String, Type> types = Collections.EMPTY_MAP;

  private Map<String, Codec> resolvedCodecs = new HashMap<>();

  /**
   * Creates a Spanner statement for a given SQL statement.
   *
   * <p>If no transaction is present, a temporary strongly consistent readonly transaction will be
   * used.
   * @param client cloud spanner client to use for performing the query operation
   * @param session current cloud spanner session
   * @param transaction current cloud spanner transaction, or empty if no transaction is started
   * @param sql the query to execute
   */
  public SpannerStatement(
      Client client,
      Session session,
      @Nullable SpannerTransactionContext transaction,
      String sql) {

    this.client = client;
    this.session = session;
    this.transaction = transaction;
    this.sql = sql;
    add();
  }

  @Override
  public Statement add() {
    this.bindings.add(new HashMap<>());
    return this;
  }

  @Override
  public Statement bind(Object identifier, Object value) {
    requireNonNull(identifier);
    if (identifier instanceof String) {
      this.bindings.getLast().put((String)identifier, value);
      return this;
    }
    throw new IllegalArgumentException("Only String identifiers are supported");
  }

  @Override
  public Statement bind(int i, Object o) {
    throw new IllegalArgumentException("Only named parameters are supported");
  }

  @Override
  public Statement bindNull(Object identifier, Class<?> type) {
    return bind(identifier, null);
  }

  @Override
  public Statement bindNull(int i, Class<?> type) {
    throw new IllegalArgumentException("Only named parameters are supported");
  }

  @Override
  public Publisher<? extends Result> execute() {
    List<Struct> paramsStructs = new ArrayList<>();
    for (Map<String, Object> bindingsBatch : this.bindings) {
      Builder paramsStructBuilder = Struct.newBuilder();
      Map<String, Type> types = this.types.isEmpty() ? new HashMap<>() : null;

      for (Map.Entry<String, Object> binding : bindingsBatch.entrySet()) {
        String paramName = binding.getKey();
        Codec codec = this.resolvedCodecs.computeIfAbsent(paramName,
            name -> this.codecs.getCodec(binding.getValue()));
        paramsStructBuilder
            .putFields(paramName, codec.encode(binding.getValue()));

        if (this.types.isEmpty()) {
          types.put(paramName,
              Type.newBuilder().setCode(codec.getTypeCode()).build());
        }

      }

      if (types != null) {
        this.types = types;
      }

      paramsStructs.add(paramsStructBuilder.build());
    }

    Flux<Struct> structFlux = Flux.fromIterable(paramsStructs);

    if (this.sql != null && this.sql.trim().toLowerCase().startsWith("select")) {
      return structFlux.flatMap(this::runSingleStatement);
    }
    return structFlux.concatMapDelayError(this::runSingleStatement);
  }

  private Mono<? extends Result> runSingleStatement(Struct params) {
    PartialResultRowExtractor partialResultRowExtractor = new PartialResultRowExtractor();

    return this.client
        .executeStreamingSql(this.session, this.transaction, this.sql, params, this.types)
        .switchOnFirst((signal, flux) -> {
          if (signal.hasError()) {
            return Mono.error(signal.getThrowable());
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
                flux.flatMapIterable(partialResultRowExtractor),
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
}
