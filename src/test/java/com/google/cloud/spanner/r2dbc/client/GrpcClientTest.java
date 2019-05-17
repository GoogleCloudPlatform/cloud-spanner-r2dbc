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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.PartialResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

/**
 * Tests the GRPC client class.
 */
public class GrpcClientTest {

  @Test
  public void assembleRowsTest() {

    // These are the expected final rows' values
    Value a1 = Value.newBuilder().setBoolValue(false).build();
    Value a2 = Value.newBuilder().setStringValue("abc").build();
    Value a3 = Value.newBuilder().setListValue(ListValue.newBuilder().addAllValues(Arrays.asList(
        Value.newBuilder().setNumberValue(12).build(),
        Value.newBuilder().setNumberValue(34).build(),
        Value.newBuilder().setNumberValue(56).build())).build()).build();

    Value b1 = Value.newBuilder().setBoolValue(true).build();
    Value b2 = Value.newBuilder().setStringValue("xyz").build();
    Value b3 = Value.newBuilder().setListValue(ListValue.newBuilder().addAllValues(Arrays.asList(
        Value.newBuilder().setNumberValue(78).build(),
        Value.newBuilder().setNumberValue(910).build(),
        Value.newBuilder().setNumberValue(1122).build())).build()).build();

    ResultSetMetadata resultSetMetadata = ResultSetMetadata.newBuilder().setRowType(
        StructType.newBuilder()
            .addFields(Field.newBuilder().setName("boolField").build())
            .addFields(Field.newBuilder().setName("stringField").build())
            .addFields(Field.newBuilder().setName("listField").build())
            .build()
    ).build();

    // The values above will be split across several partial result sets.
    PartialResultSet p1 = PartialResultSet.newBuilder().setMetadata(
        resultSetMetadata
    ).setChunkedValue(false)
        .addValues(a1).build();

    PartialResultSet p2 = PartialResultSet.newBuilder()
        .addValues(Value.newBuilder().setStringValue("a")).setChunkedValue(true).build();

    PartialResultSet p3 = PartialResultSet.newBuilder()
        .addValues(Value.newBuilder().setStringValue("b")).setChunkedValue(true).build();

    PartialResultSet p4 = PartialResultSet.newBuilder()
        .addValues(Value.newBuilder().setStringValue("c"))
        .addValues(
            Value.newBuilder().setListValue(ListValue.newBuilder().addAllValues(Arrays.asList(
                Value.newBuilder().setNumberValue(12).build(),
                Value.newBuilder().setNumberValue(34).build())).build()).build())
        .setChunkedValue(true).build();

    PartialResultSet p5 = PartialResultSet.newBuilder()
        .addValues(Value.newBuilder().setListValue(ListValue.newBuilder().addAllValues(
            Collections.singletonList(
                Value.newBuilder().setNumberValue(56).build())).build()).build())
        .addValues(Value.newBuilder().setBoolValue(true))
        .addValues(Value.newBuilder().setStringValue("xy"))
        .setChunkedValue(true).build();

    PartialResultSet p6 = PartialResultSet.newBuilder()
        .addValues(Value.newBuilder().setStringValue("z"))
        .addValues(Value.newBuilder().setListValue(ListValue.newBuilder().addAllValues(
            Collections.singletonList(
                Value.newBuilder().setNumberValue(78).build())).build()).build())
        .setChunkedValue(true).build();

    PartialResultSet p7 = PartialResultSet.newBuilder()
        .addValues(Value.newBuilder().setListValue(ListValue.newBuilder().addAllValues(
            Collections.singletonList(
                Value.newBuilder().setNumberValue(910).build())).build()).build())
        .setChunkedValue(true).build();

    PartialResultSet p8 = PartialResultSet.newBuilder()
        .addValues(Value.newBuilder().setListValue(ListValue.newBuilder().addAllValues(
            Collections.singletonList(
                Value.newBuilder().setNumberValue(1122).build())).build()).build())
        .setChunkedValue(false).build();

    Flux<PartialResultSet> inputs = Flux.just(p1, p2, p3, p4, p5, p6, p7, p8);

    Tuple2<Mono<ResultSetMetadata>, Flux<List<Value>>> results = new GrpcClient(null, null)
        .assembleRowsFromPartialResults(inputs);

    assertThat(results.getT1().block()).isEqualTo(resultSetMetadata);

    List<List<Value>> rows = results.getT2().collectList().block();

    assertThat(rows.get(0)).containsExactly(a1, a2, a3);
    assertThat(rows.get(1)).containsExactly(b1, b2, b3);
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.google.spanner.v1.CreateSessionRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.PartialResultSet;
import com.google.spanner.v1.Session;
import com.google.spanner.v1.SpannerGrpc;
import com.google.spanner.v1.SpannerGrpc.SpannerImplBase;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.function.Consumer;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Mono;

/**
 * Test for {@link GrpcClient}.
 */
public class GrpcClientTest {


  @Test
  public void testCreateSession() throws IOException {

    SpannerImplBase spannerSpy = doTest(new SpannerImplBase() {
          @Override
          public void createSession(CreateSessionRequest request,
              StreamObserver<Session> responseObserver) {
            responseObserver.onNext(Session.newBuilder().build());
            responseObserver.onCompleted();
          }
        },
        // call the method under test
        grpcClient -> grpcClient.createSession("testDb").block()
    );

    // verify the service was called correctly
    ArgumentCaptor<CreateSessionRequest> requestCaptor = ArgumentCaptor
        .forClass(CreateSessionRequest.class);
    verify(spannerSpy).createSession(requestCaptor.capture(), any());
    assertEquals("testDb", requestCaptor.getValue().getDatabase());
  }

  @Test
  public void testExecuteStreamingSql() throws IOException {

    ExecuteSqlRequest request = ExecuteSqlRequest.newBuilder().build();

    String sessionName = "/session/1234";
    Session session = Session.newBuilder().setName(sessionName).build();
    String sql = "select book from library";
    SpannerImplBase spannerSpy = doTest(new SpannerImplBase() {
          @Override
          public void executeStreamingSql(ExecuteSqlRequest request,
              StreamObserver<PartialResultSet> responseObserver) {
            responseObserver.onNext(PartialResultSet.newBuilder().build());
            responseObserver.onCompleted();
          }
        },
        // call the method under test
        grpcClient -> grpcClient.executeStreamingSql(session, Mono.empty(), sql).blockFirst()
    );

    // verify the service was called correctly
    ArgumentCaptor<ExecuteSqlRequest> requestCaptor = ArgumentCaptor
        .forClass(ExecuteSqlRequest.class);
    verify(spannerSpy).executeStreamingSql(requestCaptor.capture(), any());
    assertEquals(sql, requestCaptor.getValue().getSql());
    assertEquals(sessionName, requestCaptor.getValue().getSession());
    assertTrue(requestCaptor.getValue().getTransaction().getSingleUse().getReadOnly().getStrong());
  }

  /**
   * Starts and shuts down an in-process gRPC service based on the {@code serviceImpl} provided,
   * while allowing a test to execute using the {@link GrpcClient}.
   *
   * @param serviceImpl implementation of the Spanner service. Typically, just the methods needed to
   *     execute the test.
   * @param clientConsumer consumer of the {@link GrpcClient} - the class under test.
   * @return a Mockito spy for the gRPC service for verification.
   */
  private SpannerImplBase doTest(SpannerGrpc.SpannerImplBase serviceImpl,
      Consumer<GrpcClient> clientConsumer)
      throws IOException {
    SpannerGrpc.SpannerImplBase serviceImplSpy = spy(serviceImpl);

    String serverName = InProcessServerBuilder.generateName();

    Server server = InProcessServerBuilder
        .forName(serverName).directExecutor().addService(serviceImplSpy).build().start();

    ManagedChannel channel =
        InProcessChannelBuilder.forName(serverName).directExecutor().build();

    clientConsumer.accept(new GrpcClient(SpannerGrpc.newStub(channel)));

    channel.shutdown();
    server.shutdown();

    return serviceImplSpy;
  }
}
