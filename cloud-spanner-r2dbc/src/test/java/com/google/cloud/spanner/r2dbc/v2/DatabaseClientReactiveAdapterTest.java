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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.AsyncResultSet;
import com.google.cloud.spanner.AsyncResultSet.CallbackResponse;
import com.google.cloud.spanner.AsyncResultSet.CursorState;
import com.google.cloud.spanner.AsyncTransactionManager.TransactionContextFuture;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.r2dbc.SpannerConnectionConfiguration;
import com.google.cloud.spanner.r2dbc.v2.DatabaseClientReactiveAdapter.ResultSetReadyCallback;
import com.google.common.util.concurrent.Futures;
import com.google.spanner.v1.ExecuteSqlRequest.QueryOptions;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.OngoingStubbing;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class DatabaseClientReactiveAdapterTest {

  // DatabaseClientReactiveAdapter dependencies
  private SpannerConnectionConfiguration config;
  private Spanner mockSpannerClient;
  private DatabaseClient mockDbClient;
  private DatabaseAdminClient mockDbAdminClient;
  private DatabaseClientTransactionManager mockTxnManager;
  private ExecutorService executorService;

  private AsyncResultSet mockResultSet;
  private ReadContext mockReadContext;
  TransactionContextFuture mockTxnContextFuture;

  private DatabaseClientReactiveAdapter adapter;

  @BeforeEach
  void setup() throws Exception {
    this.config =
        new SpannerConnectionConfiguration.Builder()
            .setFullyQualifiedDatabaseName("projects/p/instances/i/databases/d")
            .setCredentials(mock(GoogleCredentials.class))
            .build();

    this.mockSpannerClient = mock(Spanner.class);
    this.mockDbClient = mock(DatabaseClient.class);
    this.mockDbAdminClient = mock(DatabaseAdminClient.class);
    this.mockTxnManager = mock(DatabaseClientTransactionManager.class);
    this.executorService = Executors.newSingleThreadExecutor();
    this.mockReadContext = mock(ReadContext.class);
    this.mockResultSet = mock(AsyncResultSet.class);
    this.mockTxnContextFuture = mock(TransactionContextFuture.class);

    when(this.mockSpannerClient.getDatabaseClient(any())).thenReturn(this.mockDbClient);
    when(this.mockSpannerClient.getDatabaseAdminClient()).thenReturn(this.mockDbAdminClient);
    when(this.mockTxnManager.beginTransaction()).thenReturn(this.mockTxnContextFuture);
    when(this.mockTxnManager.commitTransaction()).thenReturn(ApiFutures.immediateFuture(null));
    when(this.mockTxnManager.rollbackTransaction()).thenReturn(ApiFutures.immediateFuture(null));
    when(this.mockTxnManager.getReadContext()).thenReturn(this.mockReadContext);
    when(this.mockTxnManager.clearTransactionManager())
        .thenReturn(ApiFutures.immediateFuture(null));
    when(this.mockReadContext.executeQueryAsync(any())).thenReturn(this.mockResultSet);
    doAnswer(invocation -> {
      ((Runnable) invocation.getArgument(0)).run();
      return null;
    }).when(this.mockTxnContextFuture).addListener(any(), any());
    TransactionContext mockTxnContext = mock(TransactionContext.class);
    when(this.mockTxnContextFuture.isDone()).thenReturn(true);

    when(this.mockDbClient.singleUse()).thenReturn(this.mockReadContext);

    // Normally client library ResultSet implementation would invoke R2DBC driver's callback
    // as many times as needed. Here, the mock result set simulates this by running callback once.
    when(this.mockResultSet.setCallback(any(), any())).thenAnswer(invocation -> {
      ((ResultSetReadyCallback) invocation.getArgument(1)).cursorReady(this.mockResultSet);
      return Futures.immediateFuture(null);
    });

    this.adapter = new DatabaseClientReactiveAdapter(this.mockSpannerClient, this.config);
    this.adapter.setTxnManager(this.mockTxnManager);
  }

  @AfterEach
  void shutdown() {
    this.executorService.shutdownNow();
  }

  @Test
  void testChangeAutocommitCommitsCurrentTransaction() {
    when(this.mockTxnManager.isInTransaction()).thenReturn(true);
    assertThat(this.adapter.isAutoCommit()).isTrue();

    // Toggle autocommit setting.
    Mono.from(this.adapter.setAutoCommit(false)).block();
    assertThat(this.adapter.isAutoCommit()).isFalse();
    verify(this.mockTxnManager, times(1)).commitTransaction();
  }

  @Test
  void testSameAutocommitNoop() {
    when(this.mockTxnManager.isInTransaction()).thenReturn(true);
    assertThat(this.adapter.isAutoCommit()).isTrue();

    // Toggle autocommit setting.
    Mono.from(this.adapter.setAutoCommit(true)).block();
    assertThat(this.adapter.isAutoCommit()).isTrue();
    verify(this.mockTxnManager, times(0)).commitTransaction();
  }

  @Test
  void unsetQueryOptimizerResultsInDefaultQueryOptions() {
    SpannerConnectionConfiguration config = new SpannerConnectionConfiguration.Builder()
        .setFullyQualifiedDatabaseName("projects/p/instances/i/databases/d")
        .setCredentials(mock(GoogleCredentials.class))
        .build();

    DatabaseClientReactiveAdapter adapter =
        new DatabaseClientReactiveAdapter(this.mockSpannerClient, this.config);
    assertEquals(QueryOptions.getDefaultInstance(), adapter.getQueryOptions());
  }

  @Test
  void queryOptimizerPropagatesToQueryOptions() {
    SpannerConnectionConfiguration config = new SpannerConnectionConfiguration.Builder()
        .setFullyQualifiedDatabaseName("projects/p/instances/i/databases/d")
        .setCredentials(mock(GoogleCredentials.class))
        .setOptimizerVersion("2")
        .build();

    DatabaseClientReactiveAdapter adapter =
        new DatabaseClientReactiveAdapter(this.mockSpannerClient, config);
    assertEquals("2", adapter.getQueryOptions().getOptimizerVersion());
  }

  @Test
  void resultSetReadyCallbackStopsSinkOnCompletion() {
    when(this.mockResultSet.tryNext()).thenReturn(CursorState.DONE);

    StepVerifier.create(
      Flux.<SpannerClientLibraryRow>create(sink -> {
        CallbackResponse response =
            new ResultSetReadyCallback(sink, mockResultSet).cursorReady(this.mockResultSet);
        assertThat(response).isSameAs(CallbackResponse.DONE);
      })
    ).verifyComplete();
  }

  @Test
  void resultSetReadyCallbackEmitsOnOk() {
    when(this.mockResultSet.tryNext()).thenReturn(CursorState.OK);
    Struct struct = Struct.newBuilder().add(Value.string("some result")).build();
    when(this.mockResultSet.getCurrentRowAsStruct()).thenReturn(struct);


    StepVerifier.create(
        Flux.<SpannerClientLibraryRow>create(sink -> {
          ResultSetReadyCallback cb = new ResultSetReadyCallback(sink, mockResultSet);
          CallbackResponse response = cb.cursorReady(this.mockResultSet);
          assertThat(response).isSameAs(CallbackResponse.CONTINUE);
        })
    ).assertNext(r -> assertThat(r.get(1)).isEqualTo("some result")
    ).thenCancel() // without CallbackResponse.DONE signal, sink will not complete by itself.
      .verify();
  }

  @Test
  void resultSetReadyCallbackWaitsOnNotReady() {
    when(this.mockResultSet.tryNext()).thenReturn(CursorState.NOT_READY);

    StepVerifier.create(
        Flux.<SpannerClientLibraryRow>create(sink -> {
          CallbackResponse response =
              new ResultSetReadyCallback(sink, mockResultSet).cursorReady(this.mockResultSet);

          assertThat(response).isSameAs(CallbackResponse.CONTINUE);
        })
    ).thenCancel();
  }

  @Test
  void resultSetReadyCallbackSendsErrorOnException() {
    Exception exception = new RuntimeException("boom");
    when(this.mockResultSet.tryNext()).thenThrow(exception);

    StepVerifier.create(
        Flux.<SpannerClientLibraryRow>create(sink -> {
          CallbackResponse response =
              new ResultSetReadyCallback(sink, mockResultSet).cursorReady(this.mockResultSet);
          assertThat(response).isSameAs(CallbackResponse.DONE);
        })
    ).expectErrorMessage("boom")
        .verify();
  }

  @Test
  void resultSetReadyCallbackUnboundedDemand() {
    setUpResultSet("result1", "result2", "result3", "result4", "result5");

    StepVerifier.create(
            Flux.<SpannerClientLibraryRow>create(
                sink -> {
                  ResultSetReadyCallback cb = new ResultSetReadyCallback(sink, mockResultSet);
                  // more callback invocations than results available
                  for (int i = 0; i < 7; i++) {
                    cb.cursorReady(this.mockResultSet);
                  }
                })) // unbounded demand
        .expectNextMatches(r -> r.get(1, String.class).equals("result1"))
        .expectNextMatches(r -> r.get(1, String.class).equals("result2"))
        .expectNextMatches(r -> r.get(1, String.class).equals("result3"))
        .expectNextMatches(r -> r.get(1, String.class).equals("result4"))
        .expectNextMatches(r -> r.get(1, String.class).equals("result5"))
        .verifyComplete();

    // only 3 out of 5 results returned
    verify(this.mockResultSet, times(5)).getCurrentRowAsStruct();
  }

  @Test
  void resultSetReadyCallbackWithBackpressure() {
    setUpResultSet("result1", "result2", "result3", "result4", "result5");


    StepVerifier.create(
        Flux.<SpannerClientLibraryRow>create(sink -> {
          ResultSetReadyCallback cb = new ResultSetReadyCallback(sink, mockResultSet);
          // more callback invocations than results available
          for (int i = 0; i < 7; i++) {
            cb.cursorReady(this.mockResultSet);
          }
        }),
        /* initial demand of 1 */ 1)
          .expectNextMatches(r -> r.get(1, String.class).equals("result1"))
          .thenRequest(2)
          .expectNextMatches(r -> r.get(1, String.class).equals("result2"))
          .expectNextMatches(r -> r.get(1, String.class).equals("result3"))
          .thenCancel() // without CallbackResponse.DONE signal, sink will not complete by itself.
          .verify();

    // only 3 out of 5 results returned
    verify(this.mockResultSet, times(3)).getCurrentRowAsStruct();
  }

  /**
   * Mocks results corresponding to the passed in column values, assuming a single-column table.
   * Emits DONE status after emitting N (number of columns) OK statuses.
   *
   * @param columnValues assumes a single-column table
   */
  private void setUpResultSet(String... columnValues) {
    OngoingStubbing<CursorState> tryNextStub = when(this.mockResultSet.tryNext());

    for (String value : columnValues) {
      System.out.println("column " + value);
      tryNextStub = tryNextStub.thenReturn(CursorState.OK);
    }
    tryNextStub.thenReturn(CursorState.DONE);

    OngoingStubbing<Struct> getRowStub = when(this.mockResultSet.getCurrentRowAsStruct());
    for (String value : columnValues) {
      Struct struct = Struct.newBuilder()
          .add(Value.string(value))
          .build();

      getRowStub = getRowStub.thenReturn(struct);
    }
  }

  @Test
  void resultSetReadyCallback_demandStartsAtZero() {
    ResultSetReadyCallback cb = new ResultSetReadyCallback(mock(FluxSink.class), mockResultSet);
    assertThat(cb.isUnbounded()).isFalse();
    assertThat(cb.hasDemand()).isFalse();
    assertThat(cb.getDemand()).isZero();
  }

  @Test
  void resultSetReadyCallback_unboundedDemandMeansMaxLongValue() {
    ResultSetReadyCallback cb = new ResultSetReadyCallback(mock(FluxSink.class), mockResultSet);
    cb.increaseDemand(Long.MAX_VALUE);
    assertThat(cb.isUnbounded()).isTrue();
    assertThat(cb.hasDemand()).isTrue();
    assertThat(cb.getDemand()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void resultSetReadyCallback_unboundedDemandCannotDecrease() {
    ResultSetReadyCallback cb = new ResultSetReadyCallback(mock(FluxSink.class), mockResultSet);
    cb.increaseDemand(Long.MAX_VALUE);
    assertThat(cb.isUnbounded()).isTrue();
    assertThat(cb.getDemand()).isEqualTo(Long.MAX_VALUE);
    assertThat(cb.hasDemand()).isTrue();


    cb.decreaseDemand();

    assertThat(cb.isUnbounded()).isTrue();
    assertThat(cb.getDemand()).isEqualTo(Long.MAX_VALUE);
    assertThat(cb.hasDemand()).isTrue();
  }

  @Test
  void resultSetReadyCallback_demandAccumulates() {
    ResultSetReadyCallback cb = new ResultSetReadyCallback(mock(FluxSink.class), mockResultSet);
    cb.increaseDemand(17);
    assertThat(cb.isUnbounded()).isFalse();
    assertThat(cb.getDemand()).isEqualTo(17);
    assertThat(cb.hasDemand()).isTrue();

    cb.increaseDemand(25);

    assertThat(cb.isUnbounded()).isFalse();
    assertThat(cb.getDemand()).isEqualTo(42);
    assertThat(cb.hasDemand()).isTrue();
  }

  @Test
  void resultSetReadyCallback_switchFromBoundedToUnbounded() {
    ResultSetReadyCallback cb = new ResultSetReadyCallback(mock(FluxSink.class), mockResultSet);
    cb.increaseDemand(17);
    assertThat(cb.isUnbounded()).isFalse();
    assertThat(cb.getDemand()).isEqualTo(17);
    assertThat(cb.hasDemand()).isTrue();

    cb.increaseDemand(Long.MAX_VALUE);

    assertThat(cb.isUnbounded()).isTrue();
    assertThat(cb.getDemand()).isEqualTo(Long.MAX_VALUE);
    assertThat(cb.hasDemand()).isTrue();
  }

  @Test
  void resultSetReadyCallback_demandCannotFallBelowZero() {
    ResultSetReadyCallback cb = new ResultSetReadyCallback(mock(FluxSink.class), mockResultSet);
    cb.increaseDemand(2);
    assertThat(cb.isUnbounded()).isFalse();
    assertThat(cb.getDemand()).isEqualTo(2);
    assertThat(cb.hasDemand()).isTrue();

    cb.decreaseDemand();
    cb.decreaseDemand();

    assertThat(cb.isUnbounded()).isFalse();
    assertThat(cb.getDemand()).isEqualTo(0);
    assertThat(cb.hasDemand()).isFalse();

    cb.decreaseDemand();

    assertThat(cb.isUnbounded()).isFalse();
    assertThat(cb.getDemand()).isEqualTo(0);
    assertThat(cb.hasDemand()).isFalse();
  }

  @Test
  void resultSetClosedOnSuccessfulFluxCompletion() {

    // simulate no results
    when(this.mockResultSet.tryNext()).thenReturn(CursorState.DONE);

    StepVerifier.create(this.adapter.runSelectStatement(Statement.of("SELECT 1")))
        .verifyComplete();

    verify(this.mockResultSet).close();
  }

  @Test
  void resultSetCanceledOnFluxCancellation() {

    StepVerifier.create(this.adapter.runSelectStatement(Statement.of("SELECT 1")))
        .thenCancel().verify();
    verify(this.mockResultSet).cancel();
    verify(this.mockResultSet).close();

  }

  @Test
  void beginTransactionCallsDelegate() {
    StepVerifier.create(this.adapter.beginTransaction())
        .verifyComplete();

    verify(this.mockTxnManager).beginTransaction();
  }

  @Test
  void beginReadonlyTransactionCallsDelegate() {
    StepVerifier.create(this.adapter.beginReadonlyTransaction(TimestampBound.strong()))
        .verifyComplete();

    verify(this.mockTxnManager).beginReadonlyTransaction(TimestampBound.strong());
  }

  @Test
  void rollbackTransactionCallsDelegateAndClearsTransactionManager() {
    StepVerifier.create(this.adapter.rollback())
        .verifyComplete();

    verify(this.mockTxnManager).rollbackTransaction();
    verify(this.mockTxnManager).clearTransactionManager();
  }

  @Test
  void closedAdapterUnhealthy() {
    StepVerifier.create(
        Flux.concat(this.adapter.close(), this.adapter.localHealthcheck())
    ).expectNext(false).verifyComplete();
  }

  @Test
  void closedAdapterHealthcheckDoesNotNeedToCheckServer() {
    StepVerifier.create(
        Flux.concat(this.adapter.close(), this.adapter.healthCheck())
    ).expectNext(false).verifyComplete();

    verifyNoInteractions(this.mockReadContext);
  }

  @Test
  void closedSpannerClientHealthcheckDoesNotNeedToCheckServer() {
    when(this.mockSpannerClient.isClosed()).thenReturn(true);

    StepVerifier.create(this.adapter.healthCheck())
        .expectNext(false).verifyComplete();

    verifyNoInteractions(this.mockReadContext);
  }

  @Test
  void healthcheckRunsRemoteQuerySuccessfully() {
    when(this.mockSpannerClient.isClosed()).thenReturn(false);
    when(this.mockResultSet.tryNext()).thenReturn(CursorState.DONE);

    StepVerifier.create(this.adapter.healthCheck())
        .expectNext(true).verifyComplete();
  }

  @Test
  void healthcheckRunsRemoteQueryWithError() {
    when(this.mockSpannerClient.isClosed()).thenReturn(false);
    when(this.mockResultSet.tryNext()).thenThrow(new RuntimeException("nope"));

    StepVerifier.create(this.adapter.healthCheck())
        .expectNext(false).verifyComplete();
  }

  @Test
  void closingAdapterTwiceOnlyCallsDelegateOnce() {
    StepVerifier.create(
        Flux.concat(this.adapter.close(), this.adapter.close())
    ).verifyComplete();

    verify(this.mockTxnManager).clearTransactionManager();
    verifyNoMoreInteractions(this.mockTxnManager);
  }
}
