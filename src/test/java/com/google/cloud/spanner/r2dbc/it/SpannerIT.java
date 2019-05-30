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
import com.google.cloud.spanner.r2dbc.SpannerResult;
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
import java.io.IOException;
import java.time.LocalDate;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
    SpannerConnection spannerConnection = (SpannerConnection)connection.block();
    String activeSessionName = spannerConnection.getSession().getName();

    List<String> activeSessions = getSessionNames();
    assertThat(activeSessions).contains(activeSessionName);

    Mono.from(spannerConnection.close()).block();

    activeSessions = getSessionNames();
    assertThat(activeSessions).doesNotContain(activeSessionName);
  }

  @Test
  public void testQuerying() {
    Mono.from(this.connectionFactory.create())
        .delayUntil(c -> c.beginTransaction())
        .delayUntil(c -> Mono.from(c.createStatement("DELETE FROM books WHERE true").execute())
            .flatMap(r -> Mono.from(r.getRowsUpdated())))
        .delayUntil(c -> c.commitTransaction())
        .block();

    assertThat(Mono.from(this.connectionFactory.create())
        .map(connection -> connection.createStatement("Select count(1) FROM books"))
        .flatMapMany(statement -> statement.execute())
        .flatMap(spannerResult -> spannerResult.map(
            (r, meta) -> r.get(0, Long.class)
        ))
        .collectList()
        .block().get(0)).isZero();

    Mono.from(this.connectionFactory.create())
        .delayUntil(c -> c.beginTransaction())
        .delayUntil(c -> Flux.from(c.createStatement(
            "INSERT BOOKS (UUID, TITLE, AUTHOR, CATEGORY, FICTION, PUBLISHED, WORDS_PER_SENTENCE)"
                + " VALUES (@uuid, @title, @author, @category, @fiction, @published, @wps);")
            .bind("uuid", "2b2cbd78-ecd8-430e-b685-fa7910f8a4c7")
            .bind("author", "Douglas Crockford")
            .bind("category", 100L)
            .bind("title", "JavaScript: The Good Parts")
            .bind("fiction", true)
            .bind("published", LocalDate.of(2008,5,1))
            .bind("wps", 20.8)
            .add()
            .bind("uuid", "df0e3d06-2743-4691-8e51-6d33d90c5cb9")
            .bind("author", "Joshua Bloch")
            .bind("category", 100L)
            .bind("title", "Effective Java")
            .bind("fiction", false)
            .bind("published", LocalDate.of(2018,1,6))
            .bind("wps", 15.1)
            .execute()).flatMapSequential(r -> Mono.from(r.getRowsUpdated())))
        .delayUntil(c -> c.commitTransaction())
        .block();

    List<String> result = Mono.from(this.connectionFactory.create())
        .map(connection -> connection.createStatement("SELECT title, author FROM books"))
        .flatMapMany(statement -> statement.execute())
        .flatMap(spannerResult -> spannerResult.map(
            (r, meta) -> r.get(0, String.class) + " by " + r.get(1, String.class)
        ))
        .doOnNext(s -> System.out.println("Book: " + s))
        .collectList()
        .block();

    assertThat(result).containsExactlyInAnyOrder(
        "JavaScript: The Good Parts by Douglas Crockford",
        "Effective Java by Joshua Bloch");

    List<String> result2 = Mono.from(this.connectionFactory.create())
        .map(connection -> connection
            .createStatement("SELECT title, author FROM books WHERE author = @author"))
        .flatMapMany(statement -> statement
            .bind("author", "Joshua Bloch")
            .execute())
        .flatMap(spannerResult -> spannerResult.map(
            (r, meta) -> r.get(0, String.class) + " by " + r.get(1, String.class)
        ))
        .doOnNext(s -> System.out.println("Book: " + s))
        .collectList()
        .block();

    assertThat(result2).containsExactly("Effective Java by Joshua Bloch");

    List<String> result3 = Mono.from(this.connectionFactory.create())
        .map(connection -> connection
            .createStatement("SELECT title, author FROM books WHERE author = @author"))
        .flatMapMany(statement -> statement
            .bind("author", "Joshua Bloch").add()
            .bind("author", "Douglas Crockford")
            .execute())
        .flatMap(spannerResult -> spannerResult.map(
            (r, meta) -> r.get(0, String.class) + " by " + r.get(1, String.class)
        ))
        .doOnNext(s -> System.out.println("Book: " + s))
        .collectList()
        .block();

    assertThat(result3).containsExactlyInAnyOrder(
        "JavaScript: The Good Parts by Douglas Crockford",
        "Effective Java by Joshua Bloch");

    Mono<SpannerResult> deleteResult = Mono.from(this.connectionFactory.create())
        .delayUntil(c -> c.beginTransaction())
        .flatMap(c -> Mono.from(c.createStatement("DELETE FROM books WHERE true").execute()))
        .cast(SpannerResult.class);

    assertThat(deleteResult.flatMap(r -> Mono.from(r.getRowsUpdated())).block()).isEqualTo(2);
  }

  @Test
  public void testNoopUpdate() {
    Mono<SpannerResult> noopUpdateResult = Mono.from(this.connectionFactory.create())
        .delayUntil(c -> c.beginTransaction())
        .flatMap(c -> Mono.from(c.createStatement(
            "UPDATE BOOKS set author = 'blah2' where title = 'asdasdf_dont_exist'").execute()))
        .cast(SpannerResult.class);

    SpannerResult result = noopUpdateResult.block();

    int rowsUpdated = Mono.from(result.getRowsUpdated()).block();
    assertThat(rowsUpdated).isEqualTo(0);

    List<String> rowsReturned = result.map((row, metadata) -> row.toString()).collectList().block();
    assertThat(rowsReturned).isEmpty();
  }

  @Test
  public void testEmptySelect() {
    List<String> result = Mono.from(this.connectionFactory.create())
        .map(connection -> connection.createStatement(
            "SELECT title, author FROM books where author = 'Nobody P. Smith'"))
        .flatMapMany(statement -> statement.execute())
        .flatMap(spannerResult -> spannerResult.map(
            (r, meta) -> r.get(0, String.class) + " by " + r.get(1, String.class)
        ))
        .collectList()
        .block();

    assertThat(result).isEmpty();
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
