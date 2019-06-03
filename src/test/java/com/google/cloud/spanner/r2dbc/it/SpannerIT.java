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

package com.google.cloud.spanner.r2dbc.it;

import static com.google.cloud.spanner.r2dbc.SpannerConnectionFactoryProvider.INSTANCE;
import static com.google.cloud.spanner.r2dbc.client.GrpcClient.HOST;
import static com.google.cloud.spanner.r2dbc.client.GrpcClient.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.spanner.r2dbc.SpannerConnection;
import com.google.cloud.spanner.r2dbc.SpannerConnectionFactory;
import com.google.cloud.spanner.r2dbc.util.ObservableReactiveUtil;
import com.google.spanner.v1.DatabaseName;
import com.google.spanner.v1.ListSessionsRequest;
import com.google.spanner.v1.ListSessionsResponse;
import com.google.spanner.v1.Session;
import com.google.spanner.v1.SpannerGrpc;
import com.google.spanner.v1.SpannerGrpc.SpannerStub;
import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.auth.MoreCallCredentials;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import java.io.IOException;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Integration test for connecting to a real Spanner instance.
 */
public class SpannerIT {

  private static final String DRIVER_NAME = "spanner";

  private static final String TEST_INSTANCE = "reactivetest";

  private static final String TEST_DATABASE = "testdb";

  private static final ConnectionFactory connectionFactory =
      ConnectionFactories.get(ConnectionFactoryOptions.builder()
          // TODO: consider whether to bring autodiscovery of project ID
          .option(Option.valueOf("project"), ServiceOptions.getDefaultProjectId())
          .option(DRIVER, DRIVER_NAME)
          .option(INSTANCE, TEST_INSTANCE)
          .option(DATABASE, TEST_DATABASE)
          .build());

  private SpannerStub spanner;

  /**
   * Setup the Spanner stub for testing.
   */
  @Before
  public void setupStubs() throws IOException {
    // Create a channel
    ManagedChannel channel = ManagedChannelBuilder
        .forAddress(HOST, PORT)
        .build();

    // Create blocking and async stubs using the channel
    CallCredentials callCredentials = MoreCallCredentials
        .from(GoogleCredentials.getApplicationDefault());

    // Create the asynchronous stub for Cloud Spanner
    this.spanner = SpannerGrpc.newStub(channel)
        .withCallCredentials(callCredentials);
  }

  @Test
  public void testSessionManagement() {
    assertThat(this.connectionFactory).isInstanceOf(SpannerConnectionFactory.class);

    Mono<Connection> connection = (Mono<Connection>) this.connectionFactory.create();
    SpannerConnection spannerConnection = (SpannerConnection) connection.block();
    String activeSessionName = spannerConnection.getSession().getName();

    List<String> activeSessions = getSessionNames();
    assertThat(activeSessions).contains(activeSessionName);

    Mono.from(spannerConnection.close()).block();

    activeSessions = getSessionNames();
    assertThat(activeSessions).doesNotContain(activeSessionName);
  }

  @Test
  public void testQuerying() {
    executeDmlQuery("DELETE FROM books WHERE true");

    StepVerifier.create(executeReadQuery(
        "Select count(1) as count FROM books",
        (row, rowMetadata) -> row.get("count", Long.class)))
        .expectNext(0L)
        .verifyComplete();

    executeDmlQuery(
        "INSERT BOOKS (UUID, TITLE, AUTHOR, CATEGORY) VALUES"
            + " ('df0e3d06-2743-4691-8e51-6d33d90c5cb9', 'Effective Java', "
            + "'Joshua Bloch', 100)");

    executeDmlQuery(
        "INSERT BOOKS (UUID, TITLE, AUTHOR, CATEGORY) VALUES"
            + " ('2b2cbd78-ecd8-430e-b685-fa7910f8a4c7', 'JavaScript: "
            + "The Good Parts', 'Douglas Crockford', 100);");

    StepVerifier.create(executeReadQuery(
        "SELECT title, author FROM books",
        (r, meta) -> r.get(0, String.class) + " by " + r.get(1, String.class)))
        .expectNext("JavaScript: The Good Parts by Douglas Crockford",
            "Effective Java by Joshua Bloch")
        .verifyComplete();

    StepVerifier.create(executeDmlQuery("DELETE FROM books WHERE true"))
        .expectNext(2)
        .verifyComplete();
  }

  @Test
  public void testNoopUpdate() {
    Mono<Result> result = Mono.from(connectionFactory.create())
        .delayUntil(c -> c.beginTransaction())
        .flatMap(c -> Mono.from(c.createStatement(
            "UPDATE BOOKS set author = 'blah2' where title = 'asdasdf_dont_exist'").execute()));

    StepVerifier.create(result.flatMap(result1 -> Mono.from(result1.getRowsUpdated())))
        .expectNext(0)
        .verifyComplete();

    StepVerifier.create(
        result.flatMap(result1 -> Mono.from(result1.map((row, rowMetadata) -> row.toString()))))
        .verifyComplete();
  }

  @Test
  public void testEmptySelect() {
    Flux<String> results = executeReadQuery(
        "SELECT title, author FROM books where author = 'Nobody P. Smith'",
        (r, meta) -> r.get(0, String.class));

    StepVerifier.create(results)
        .verifyComplete();
  }

  /**
   * Executes a DML query and returns the rows updated.
   */
  private Mono<Integer> executeDmlQuery(String sql) {
    return Mono.from(connectionFactory.create())
        .flatMap(connection -> {
          connection.beginTransaction();
          Mono<Integer> rowsUpdated = Mono.from(connection.createStatement(sql).execute())
              .flatMap(result -> Mono.from(result.getRowsUpdated()));
          connection.commitTransaction();
          return rowsUpdated;
        });
  }

  /**
   * Executes a read query and runs the provided {@code mappingFunction} on the elements returned.
   */
  private <T> Flux<T> executeReadQuery(
      String sql,
      BiFunction<Row, RowMetadata, T> mappingFunction) {

    return Mono.from(connectionFactory.create())
        .map(connection -> connection.createStatement(sql))
        .flatMapMany(Statement::execute)
        .flatMap(spannerResult -> spannerResult.map(mappingFunction));
  }

  private List<String> getSessionNames() {
    String databaseName =
        DatabaseName.format(ServiceOptions.getDefaultProjectId(), TEST_INSTANCE, TEST_DATABASE);

    ListSessionsRequest listSessionsRequest =
        ListSessionsRequest.newBuilder()
            .setDatabase(databaseName)
            .build();

    ListSessionsResponse listSessionsResponse =
        ObservableReactiveUtil.<ListSessionsResponse>unaryCall(
            obs -> this.spanner.listSessions(listSessionsRequest, obs))
            .block();

    return listSessionsResponse.getSessionsList()
        .stream()
        .map(Session::getName)
        .collect(Collectors.toList());
  }
}
