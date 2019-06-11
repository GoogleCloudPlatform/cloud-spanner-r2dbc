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
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.r2dbc.client.Client;
import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.spanner.v1.CommitResponse;
import com.google.spanner.v1.PartialResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.Session;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Transaction;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import io.r2dbc.spi.Statement;
import java.util.Collections;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.PublisherProbe;

/**
 * Test for {@link SpannerConnection}.
 */
public class SpannerConnectionTest {

  private static final Session TEST_SESSION =
      Session.newBuilder().setName("project/session/1234").build();
  static final Struct EMPTY_STRUCT = Struct.newBuilder().build();
  static final Map<String, Type> EMPTY_TYPE_MAP = Collections.emptyMap();

  private Client mockClient;

  /**
   * Initializes the mocks in the test.
   */
  @Before
  public void setupMocks() {
    this.mockClient = mock(Client.class);

    when(this.mockClient.beginTransaction(any(ExecutionContext.class)))
        .thenReturn(Mono.just(Transaction.getDefaultInstance()));
    when(this.mockClient.commitTransaction(any(ExecutionContext.class)))
        .thenReturn(Mono.just(CommitResponse.getDefaultInstance()));
    when(this.mockClient.rollbackTransaction(any(ExecutionContext.class)))
        .thenReturn(Mono.empty());
  }

  @Test
  public void executeStatementReturnsWorkingStatementWithCorrectQuery() {

    SpannerConnection connection
        = new SpannerConnection(this.mockClient, TEST_SESSION);
    connection.setPartialResultSetFetchSize(1);
    String sql = "select book from library";

    when(this.mockClient.executeStreamingSql(
          any(ExecutionContext.class), eq(sql), eq(EMPTY_STRUCT), eq(EMPTY_TYPE_MAP)))
        .thenReturn(Flux.just(makeBookPrs("Odyssey")));

    Statement statement = connection.createStatement(sql);
    assertThat(statement).isInstanceOf(SpannerStatement.class);

    StepVerifier.create(
        ((Flux<SpannerResult>)statement.execute())
            .flatMap(res -> res.map((r, m) -> (String) r.get(0))))
        .expectNext("Odyssey")
        .expectComplete()
        .verify();

    verify(this.mockClient).executeStreamingSql(any(ExecutionContext.class), eq(sql),
        eq(EMPTY_STRUCT), eq(EMPTY_TYPE_MAP));
  }

  @Test
  public void noopCommitTransactionWhenTransactionNotStarted() {
    SpannerConnection connection = new SpannerConnection(this.mockClient, TEST_SESSION);

    // No-op commit when connection is not started.
    Mono.from(connection.commitTransaction()).block();
    verify(this.mockClient, never()).commitTransaction(any(ExecutionContext.class));
  }

  @Test
  public void beginAndCommitTransactions() {
    SpannerConnection connection = new SpannerConnection(this.mockClient, TEST_SESSION);

    PublisherProbe<Transaction> beginTransactionProbe = PublisherProbe.of(
        Mono.just(Transaction.getDefaultInstance()));
    PublisherProbe<CommitResponse> commitTransactionProbe = PublisherProbe.of(
        Mono.just(CommitResponse.getDefaultInstance()));

    when(this.mockClient.beginTransaction(any(ExecutionContext.class)))
        .thenReturn(beginTransactionProbe.mono());
    when(this.mockClient.commitTransaction(any(ExecutionContext.class)))
        .thenReturn(commitTransactionProbe.mono());

    Mono.from(connection.beginTransaction())
            .then(Mono.from(connection.commitTransaction()))
            .subscribe();
    verify(this.mockClient, times(1))
        .beginTransaction(any(ExecutionContext.class));
    verify(this.mockClient, times(1))
        .commitTransaction(any(ExecutionContext.class));

    beginTransactionProbe.assertWasSubscribed();
    commitTransactionProbe.assertWasSubscribed();
  }

  @Test
  public void rollbackTransactions() {
    SpannerConnection connection = new SpannerConnection(this.mockClient, TEST_SESSION);

    PublisherProbe<Transaction> beginTransactionProbe = PublisherProbe.of(
        Mono.just(Transaction.getDefaultInstance()));
    PublisherProbe<Void> rollbackProbe = PublisherProbe.empty();

    when(this.mockClient.beginTransaction(any(ExecutionContext.class)))
        .thenReturn(beginTransactionProbe.mono());
    when(this.mockClient.rollbackTransaction(any(ExecutionContext.class)))
        .thenReturn(rollbackProbe.mono());

    Mono.from(connection.rollbackTransaction()).block();
    verify(this.mockClient, never()).rollbackTransaction(any(ExecutionContext.class));

    Mono.from(connection.beginTransaction()).block();
    Mono.from(connection.rollbackTransaction()).block();
    verify(this.mockClient, times(1)).beginTransaction(any(ExecutionContext.class));
    verify(this.mockClient, times(1)).rollbackTransaction(any(ExecutionContext.class));

    beginTransactionProbe.assertWasSubscribed();
    rollbackProbe.assertWasSubscribed();
  }

  @Test
  public void setPartialResultSetFetchSizePropagatesToStatement() {
    SpannerConnection connection = new SpannerConnection(this.mockClient, TEST_SESSION);
    connection.setPartialResultSetFetchSize(42);
    SpannerStatement statement = connection.createStatement("SELECT 1");
    assertThat(statement.getPartialResultSetFetchSize()).isEqualTo(42);
  }

  @Test
  public void nullPartialResultSetFetchSizeLeavesStatementDefault() {
    SpannerConnection connection = new SpannerConnection(this.mockClient, TEST_SESSION);
    SpannerStatement statement = connection.createStatement("SELECT 1");
    assertThat(statement.getPartialResultSetFetchSize()).isEqualTo(1);
  }

  @Test
  public void executionContextHasCorrectSessionName() {
    SpannerConnection connection = new SpannerConnection(
        this.mockClient, Session.newBuilder().setName("session-name").build());
    assertThat(connection.getExecutionContext().getSessionName()).isEqualTo("session-name");
  }

  @Test
  public void executionContextDoesNotHaveTransactionWhenInitialized() {
    SpannerConnection connection = new SpannerConnection(this.mockClient, TEST_SESSION);
    assertThat(connection.getExecutionContext().getTransactionId()).isNull();
  }

  @Test
  public void executionContextHasCorrectTransactionIdWhenTransactionSet() {
    SpannerConnection connection = new SpannerConnection(this.mockClient, TEST_SESSION);
    ByteString transactionId = ByteString.copyFrom("transaction-id".getBytes());

    when(this.mockClient.beginTransaction(any(ExecutionContext.class)))
        .thenReturn(Mono.just(Transaction.newBuilder().setId(transactionId).build()));
    when(this.mockClient.rollbackTransaction(any(ExecutionContext.class)))
        .thenReturn(Mono.empty());

    StepVerifier.create(connection.beginTransaction()).verifyComplete();
    assertThat(connection.getExecutionContext().getTransactionId()).isEqualTo(transactionId);

    StepVerifier.create(connection.rollbackTransaction()).verifyComplete();
    assertThat(connection.getExecutionContext().getTransactionId()).isNull();
  }

  @Test
  public void nextSeqNumIsSequential() {
    SpannerConnection connection = new SpannerConnection(this.mockClient, TEST_SESSION);
    long prevNum = connection.getExecutionContext().nextSeqNum();

    for (int i = 0; i < 9; i++) {
      long num = connection.getExecutionContext().nextSeqNum();
      System.out.println(num);

      if (num <= prevNum) {
        fail("Expected to be monotonically increasing; received " + prevNum + ", then " + num);
      }
      prevNum = num;
    }
  }

  private PartialResultSet makeBookPrs(String bookName) {
    return PartialResultSet.newBuilder()
        .setMetadata(ResultSetMetadata.newBuilder().setRowType(StructType.newBuilder()
            .addFields(
                Field.newBuilder().setName("book")
                    .setType(Type.newBuilder().setCode(TypeCode.STRING)))))
        .addValues(Value.newBuilder().setStringValue(bookName))
        .build();
  }
}
