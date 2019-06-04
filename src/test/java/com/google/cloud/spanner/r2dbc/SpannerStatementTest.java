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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.r2dbc.client.Client;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.spanner.v1.PartialResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.ResultSetStats;
import com.google.spanner.v1.Session;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import io.r2dbc.spi.Result;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Test for {@link SpannerStatement}.
 */
public class SpannerStatementTest {

  private static final Session TEST_SESSION =
      Session.newBuilder().setName("project/session/1234").build();

  private final Client mockClient = mock(Client.class);

  private final Value a1 = Value.newBuilder().setBoolValue(false).build();
  private final Value a2 = Value.newBuilder().setStringValue("abc").build();

  private final ResultSetMetadata resultSetMetadata = ResultSetMetadata.newBuilder().setRowType(
      StructType.newBuilder()
          .addFields(Field.newBuilder().setName("boolField")
              .setType(Type.newBuilder().setCode(TypeCode.BOOL).build()).build())
          .addFields(Field.newBuilder().setName("stringField")
              .setType(Type.newBuilder().setCode(TypeCode.STRING).build()).build())
          .build()
  ).build();

  @Test
  public void executeDummyImplementation() {

    Client mockClient = mock(Client.class);
    String sql = "select book from library";
    PartialResultSet partialResultSet = PartialResultSet.newBuilder()
        .setMetadata(ResultSetMetadata.newBuilder().setRowType(StructType.newBuilder()
            .addFields(
                Field.newBuilder().setName("book")
                    .setType(Type.newBuilder().setCode(TypeCode.STRING)))))
        .addValues(Value.newBuilder().setStringValue("Odyssey"))
        .build();
    when(mockClient.executeStreamingSql(eq(TEST_SESSION), isNull(), eq(sql), any(), any()))
        .thenReturn(Flux.just(partialResultSet));

    SpannerStatement statement
        = new SpannerStatement(mockClient, TEST_SESSION, null, sql);

    Flux<SpannerResult> result = (Flux<SpannerResult>)statement.execute();

    assertThat(result).isNotNull();

    assertThat(result.next().block().map((r, m) -> (String)r.get(0)).blockFirst())
        .isEqualTo("Odyssey");

    verify(mockClient).executeStreamingSql(eq(TEST_SESSION), isNull(), eq(sql), any(), any());
  }

  @Test
  public void executeDummyImplementationBind() {

    Client mockClient = mock(Client.class);
    //set up mock results
    PartialResultSet partialResultSet1 = PartialResultSet.newBuilder()
        .setMetadata(ResultSetMetadata.newBuilder().setRowType(StructType.newBuilder()
            .addFields(
                Field.newBuilder().setName("book")
                    .setType(Type.newBuilder().setCode(TypeCode.STRING)))))
        .addValues(Value.newBuilder().setStringValue("Odyssey"))
        .build();

    PartialResultSet partialResultSet2 = PartialResultSet.newBuilder()
        .setMetadata(ResultSetMetadata.newBuilder().setRowType(StructType.newBuilder()
            .addFields(
                Field.newBuilder().setName("book")
                    .setType(Type.newBuilder().setCode(TypeCode.STRING)))))
        .addValues(Value.newBuilder().setStringValue("Fables"))
        .build();

    String sql = "select book from library where id = @id";

    //set up mock client to return the results
    Map<String, Type> types = new HashMap<>();
    types.put("id", Type.newBuilder().setCode(TypeCode.STRING).build());
    Struct idBinding1 = Struct.newBuilder()
        .putFields("id", Value.newBuilder().setStringValue("b1").build()).build();
    when(mockClient.executeStreamingSql(TEST_SESSION, null, sql,
        idBinding1, types))
        .thenReturn(Flux.just(partialResultSet1));

    Struct idBinding2 = Struct.newBuilder().putFields("id", Value.newBuilder()
        .setStringValue("b2").build()).build();
    when(mockClient.executeStreamingSql(TEST_SESSION, null, sql,
        idBinding2, types))
        .thenReturn(Flux.just(partialResultSet2));

    //execute query
    SpannerStatement statement
        = new SpannerStatement(mockClient, TEST_SESSION, null, sql);

    Flux<SpannerResult> result = (Flux<SpannerResult>)statement
        .bind("id", "b1").add()
        .bind("id", "b2")
        .execute();

    StepVerifier.create(result)
        .expectNextMatches(spannerResult -> firstColumIsEqualTo(spannerResult, "Odyssey"))
        .expectNextMatches(spannerResult -> firstColumIsEqualTo(spannerResult, "Fables"))
        .verifyComplete();

    verify(mockClient, times(1)).executeStreamingSql(TEST_SESSION, null, sql,
        idBinding1, types);
    verify(mockClient, times(1)).executeStreamingSql(TEST_SESSION, null, sql,
        idBinding2, types);
  }

  private boolean firstColumIsEqualTo(SpannerResult spannerResult, String pattern) {
    return spannerResult.map((row, rowMetadata) -> (String)row.get(0)).blockFirst().equals(pattern);
  }

  @Test
  public void readOneResultSetQueryTest() {
    PartialResultSet p1 = PartialResultSet.newBuilder().setMetadata(
        this.resultSetMetadata
    ).setChunkedValue(false)
        .addValues(this.a1)
        .addValues(this.a2).build();

    Flux<PartialResultSet> inputs = Flux.just(p1);

    when(this.mockClient.executeStreamingSql(any(), any(), any(), any(), any())).thenReturn(inputs);

    Mono<Result> resultMono = Mono
        .from(new SpannerStatement(this.mockClient, null, null, "").execute());

    assertThat(resultMono.flatMap(r -> Mono.from(r.getRowsUpdated())).block()).isZero();
    assertThat(resultMono.flatMapMany(r -> r
        .map((row, meta) -> row.get(0, Boolean.class).toString() + "-" + row.get(1, String.class)))
        .collectList().block()).containsExactly("false-abc");
  }

  @Test
  public void readMultiResultSetQueryTest() {
    PartialResultSet p1 = PartialResultSet.newBuilder().setMetadata(
        this.resultSetMetadata
    ).setChunkedValue(false)
        .addValues(this.a1).build();

    PartialResultSet p2 = PartialResultSet.newBuilder().setChunkedValue(false)
        .addValues(this.a2).build();

    Flux<PartialResultSet> inputs = Flux.just(p1, p2);

    when(this.mockClient.executeStreamingSql(any(), any(), any(), any(), any())).thenReturn(inputs);

    assertThat(Mono.from(new SpannerStatement(this.mockClient, null, null, "").execute())
        .flatMap(r -> Mono.from(r.getRowsUpdated())).block()).isZero();
  }

  @Test
  public void readDmlQueryTest() {
    PartialResultSet p1 = PartialResultSet.newBuilder().setStats(
        ResultSetStats.newBuilder().setRowCountExact(555).build()
    ).build();

    Flux<PartialResultSet> inputs = Flux.just(p1);

    when(this.mockClient.executeStreamingSql(any(), any(), any(), any(), any())).thenReturn(inputs);

    assertThat(Mono.from(new SpannerStatement(this.mockClient, null, null, "").execute())
        .flatMap(r -> Mono.from(r.getRowsUpdated())).block()).isEqualTo(555);
  }

  @Test
  public void noopMapOnUpdateQueriesWhenNoRowsAffected() {
    Client mockClient = mock(Client.class);
    String sql = "delete from Books where true";
    PartialResultSet partialResultSet = PartialResultSet.newBuilder()
        .setMetadata(ResultSetMetadata.getDefaultInstance())
        .setStats(ResultSetStats.getDefaultInstance())
        .build();
    when(mockClient.executeStreamingSql(TEST_SESSION, null, sql,
        Struct.newBuilder().build(), Collections.EMPTY_MAP))
        .thenReturn(Flux.just(partialResultSet));

    SpannerStatement statement
        = new SpannerStatement(mockClient, TEST_SESSION, null, sql);

    SpannerResult result = ((Flux<SpannerResult>) statement.execute()).next().block();

    List<String> rowStrings = result.map((r, m) -> (String)r.get(0)).collectList().block();
    assertThat(rowStrings).isEmpty();

    int rowsUpdated = Mono.from(result.getRowsUpdated()).block();
    assertThat(rowsUpdated).isEqualTo(0);

    verify(mockClient, times(1)).executeStreamingSql(TEST_SESSION, null, sql,
        Struct.newBuilder().build(), Collections.EMPTY_MAP);
  }
}
