/*
 * Copyright 2021-2025 Google LLC
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

package com.google.cloud.spanner.r2dbc.v2;

import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.MockSpannerServiceImpl;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.r2dbc.SpannerConnectionFactoryProvider;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.RollbackRequest;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.r2dbc.spi.Closeable;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** End-to-end tests that run against an in-memory mock Spanner server. */
public class MockServerTest {

  private static Server server;
  private static MockSpannerServiceImpl mockSpanner;

  @BeforeAll
  static void setupMockSpanner() throws Exception {
    // Create a mock Spanner service and add a simple query result.
    mockSpanner = new MockSpannerServiceImpl();
    mockSpanner.setAbortProbability(0.0);
    mockSpanner.putStatementResult(
        StatementResult.query(Statement.of("SELECT 1 AS C"),
        ResultSet.newBuilder()
            .setMetadata(ResultSetMetadata
                .newBuilder()
                .setRowType(
                    StructType.newBuilder().addFields(
                        Field.newBuilder()
                            .setName("C")
                            .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                            .build()).build())
            .build())
            .addRows(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("1").build())
                    .build())
            .build()));

    // Start an in-memory gRPC server and add the Spanner service.
    InetSocketAddress address = new InetSocketAddress("localhost", 0);
    server = NettyServerBuilder.forAddress(address).addService(mockSpanner).build().start();
  }

  @AfterAll
  static void teardownMockSpanner() throws Exception {
    server.shutdown();
    server.awaitTermination();
  }

  @AfterEach
  void resetMockSpanner() {
    mockSpanner.reset();
  }

  ConnectionFactory createConnectionFactory() {
    ConnectionFactoryOptions options = ConnectionFactoryOptions.builder()
        .option(DRIVER, "cloudspanner")
        .option(SpannerConnectionFactoryProvider.PROJECT, "p")
        .option(SpannerConnectionFactoryProvider.INSTANCE, "i")
        .option(ConnectionFactoryOptions.DATABASE, "d")
        .option(ConnectionFactoryOptions.HOST, "localhost")
        .option(ConnectionFactoryOptions.PORT, server.getPort())
        .option(SpannerConnectionFactoryProvider.GOOGLE_CREDENTIALS, NoCredentials.getInstance())
        .option(SpannerConnectionFactoryProvider.USE_PLAIN_TEXT, true).build();
    return ConnectionFactories.get(options);
  }

  @Test
  void testSimpleQuery() {
    ConnectionFactory connectionFactory = createConnectionFactory();
    Publisher<? extends Connection> connectionPublisher = connectionFactory.create();
    Connection connection = Mono.from(connectionPublisher).block();

    List<String> results = Flux.from(connection.createStatement("SELECT 1 AS C").execute())
        .flatMap(spannerResult -> spannerResult.map(this::describeRow))
        .collectList().block();
    assertNotNull(results);
    assertEquals(1, results.size());
    String row = results.get(0);
    assertEquals("{\n" + "\t\"C\": 1,\n" + "}", row);

    Mono.from(((Closeable) connectionFactory).close()).subscribe();

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertFalse(request.hasTransaction());
  }

  @Test
  public void testSimpleTransaction() {
    String sql = "insert into foo (id) values (1)";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql), 1L));

    ConnectionFactory connectionFactory = createConnectionFactory();
    Publisher<? extends Connection> connectionPublisher = connectionFactory.create();
    Connection connection = Mono.from(connectionPublisher).block();

    Flux.concat(
        connection.beginTransaction(),
        connection.createStatement(sql).execute(),
        connection.commitTransaction())
        .blockLast();

    Mono.from(((Closeable) connectionFactory).close()).subscribe();

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    ExecuteSqlRequest request = mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).get(0);
    assertTrue(request.hasTransaction());
    assertTrue(request.getTransaction().hasBegin());
    assertTrue(request.getTransaction().getBegin().hasReadWrite());
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testTwoTransactions() {
    String sql = "insert into foo (id) values (1)";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql), 1L));

    ConnectionFactory connectionFactory = createConnectionFactory();
    Publisher<? extends Connection> connectionPublisher = connectionFactory.create();
    Connection connection = Mono.from(connectionPublisher).block();

    Flux.concat(connection.beginTransaction(), connection.createStatement(sql).execute(),
        connection.commitTransaction(), connection.beginTransaction(),
        connection.createStatement(sql).execute(), connection.commitTransaction()).blockLast();

    Mono.from(((Closeable) connectionFactory).close()).subscribe();

    assertEquals(2, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    for (ExecuteSqlRequest request : mockSpanner.getRequestsOfType(ExecuteSqlRequest.class)) {
      assertTrue(request.hasTransaction());
      assertTrue(request.getTransaction().hasBegin());
      assertTrue(request.getTransaction().getBegin().hasReadWrite());
    }
    assertEquals(2, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testRollbackEmptyTransaction() {
    String sql = "insert into foo (id) values (1)";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql), 1L));

    ConnectionFactory connectionFactory = createConnectionFactory();
    Publisher<? extends Connection> connectionPublisher = connectionFactory.create();
    Connection connection = Mono.from(connectionPublisher).block();

    Flux.concat(connection.beginTransaction(), connection.rollbackTransaction(),
        connection.beginTransaction(), connection.createStatement(sql).execute(),
        connection.commitTransaction()).blockLast();

    Mono.from(((Closeable) connectionFactory).close()).subscribe();

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    for (ExecuteSqlRequest request : mockSpanner.getRequestsOfType(ExecuteSqlRequest.class)) {
      assertTrue(request.hasTransaction());
      assertTrue(request.getTransaction().hasBegin());
      assertTrue(request.getTransaction().getBegin().hasReadWrite());
    }
    // Rolling back an empty transaction is a no-op and should not lead to a request on Spanner.
    assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
    assertEquals(1, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testRollbackTransaction() {
    String sql = "insert into foo (id) values (1)";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql), 1L));

    ConnectionFactory connectionFactory = createConnectionFactory();
    Publisher<? extends Connection> connectionPublisher = connectionFactory.create();
    Connection connection = Mono.from(connectionPublisher).block();

    Flux.concat(connection.beginTransaction(), connection.createStatement(sql).execute(),
        connection.rollbackTransaction()).blockLast();

    Mono.from(((Closeable) connectionFactory).close()).subscribe();

    assertEquals(1, mockSpanner.countRequestsOfType(ExecuteSqlRequest.class));
    for (ExecuteSqlRequest request : mockSpanner.getRequestsOfType(ExecuteSqlRequest.class)) {
      assertTrue(request.hasTransaction());
      assertTrue(request.getTransaction().hasBegin());
      assertTrue(request.getTransaction().getBegin().hasReadWrite());
    }
    assertEquals(1, mockSpanner.countRequestsOfType(RollbackRequest.class));
    assertEquals(0, mockSpanner.countRequestsOfType(CommitRequest.class));
  }

  @Test
  public void testRollbackEmptyTransactionAndExecuteSqlInParallel() throws Exception {
    // Redirect log to /dev/null.
    PrintStream err = System.err;
    System.setErr(new PrintStream(new OutputStream() {
      @Override
      public void write(int b) throws IOException {
        // do nothing
      }
    }));
    try {
      String sql = "insert into foo (id) values (1)";
      mockSpanner.putStatementResult(StatementResult.update(Statement.of(sql), 1L));

      ConnectionFactory connectionFactory = createConnectionFactory();
      Publisher<? extends Connection> connectionPublisher1 = connectionFactory.create();
      Connection connection1 = Mono.from(connectionPublisher1).block();
      Publisher<? extends Connection> connectionPublisher2 = connectionFactory.create();
      Connection connection2 = Mono.from(connectionPublisher2).block();

      int n = 100;
      ExecutorService rollbackService = Executors.newSingleThreadExecutor();
      ExecutorService queryService = Executors.newSingleThreadExecutor();
      Future<?> rollbackFuture = rollbackService.submit(() -> {
        repeat(n, () -> {
          Flux.concat(connection1.beginTransaction(), connection1.rollbackTransaction())
              .blockLast();
        });
      });
      Future<?> queryFuture = queryService.submit(() -> {
        repeat(n, () -> {
          Flux.concat(connection2.createStatement(sql).execute()).blockLast();
        });
      });
      rollbackFuture.get();
      queryFuture.get();

      Mono.from(((Closeable) connectionFactory).close()).block();

      assertEquals(n, mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
          .filter(request -> request.getSql().equals(sql)).count());
      // Rolling back an empty transaction is a no-op and should not lead to a request on Spanner.
      assertEquals(0, mockSpanner.countRequestsOfType(RollbackRequest.class));
      // Each of the DML statements should be committed.
      assertEquals(n, mockSpanner.countRequestsOfType(CommitRequest.class));
    } finally {
      System.setErr(err);
    }
  }

  String describeRow(Row row, RowMetadata meta) {
    StringBuilder result = new StringBuilder("{\n");
    for (ColumnMetadata col : meta.getColumnMetadatas()) {
      result.append('\t').append('"').append(col.getName()).append('"').append(':').append(' ');
      result.append(row.get(col.getName())).append(',').append('\n');
    }
    result.append('}');
    return result.toString();
  }

  static void repeat(int n, Runnable action) {
    for (int i = 0; i < n; i++) {
      action.run();
    }
  }
}
