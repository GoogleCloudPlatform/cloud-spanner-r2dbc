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

package com.google.cloud.spanner.r2dbc.v2;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class SpannerClientLibraryStatementTest {

  DatabaseClientReactiveAdapter mockAdapter;

  static Struct SINGLE_COLUMN_STRUCT1 = Struct.newBuilder().add(Value.string("resultA")).build();
  static Struct SINGLE_COLUMN_STRUCT2 = Struct.newBuilder().add(Value.string("resultB")).build();

  @BeforeEach
  public void setUpAdapterResponse() {
    this.mockAdapter = mock(DatabaseClientReactiveAdapter.class);
  }

  @Test
  public void executeSingleNoRowsUpdated() {
    when(this.mockAdapter.runSelectStatement(any(Statement.class)))
        .thenReturn(Flux.just(new SpannerClientLibraryRow(SINGLE_COLUMN_STRUCT1)));

    SpannerClientLibraryStatement statement =
        new SpannerClientLibraryStatement(this.mockAdapter, "SELECT whatever FROM unused");

    StepVerifier.create(
        Flux.from(statement.execute()).flatMap(result -> result.getRowsUpdated()))
        // SELECT statements never have updated row counts.
        .verifyComplete();

    StepVerifier.create(
        Flux.from(statement.execute()).flatMap(
            result -> result.map((r, rm) -> r.get(1, String.class))))
        .expectNext("resultA")
        .verifyComplete();
  }

  @Test
  public void executeMultipleReturnsExpectedValuesAndCallsAdapterForEachParameterizedSelect() {

    String query = "SELECT * from table WHERE column=%col1";
    Statement expectedSpannerStatement1 = Statement.newBuilder(query)
        .bind("col1").to("val1").build();
    Statement expectedSpannerStatement2 = Statement.newBuilder(query)
        .bind("col1").to("val2").build();

    when(this.mockAdapter.runSelectStatement(eq(expectedSpannerStatement1)))
        .thenReturn(Flux.just(new SpannerClientLibraryRow(SINGLE_COLUMN_STRUCT1)));
    when(this.mockAdapter.runSelectStatement(eq(expectedSpannerStatement2)))
        .thenReturn(Flux.just(new SpannerClientLibraryRow(SINGLE_COLUMN_STRUCT2)));

    io.r2dbc.spi.Statement statement =
        new SpannerClientLibraryStatement(this.mockAdapter, query)
        .bind("col1", "val1").add()
        .bind("col1", "val2");

    StepVerifier.create(
        Flux.from(statement.execute()).flatMapSequential(
            result -> result.map((r, rm) -> r.get(1, String.class))))
        .expectNext("resultA", "resultB")
        .verifyComplete();

    verify(this.mockAdapter).runSelectStatement(eq(expectedSpannerStatement1));
    verify(this.mockAdapter).runSelectStatement(eq(expectedSpannerStatement2));
    verifyNoMoreInteractions(this.mockAdapter);

  }
}
