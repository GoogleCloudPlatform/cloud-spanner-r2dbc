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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.AsyncResultSet;
import com.google.cloud.spanner.AsyncResultSet.CallbackResponse;
import com.google.cloud.spanner.AsyncResultSet.CursorState;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.r2dbc.SpannerConnectionConfiguration;
import com.google.cloud.spanner.r2dbc.v2.DatabaseClientReactiveAdapter.ResultSetReadyCallback;
import com.google.spanner.v1.ExecuteSqlRequest.QueryOptions;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

class DatabaseClientReactiveAdapterTest {

  // DatabaseClientReactiveAdapter dependencies
  private SpannerConnectionConfiguration config;
  private Spanner mockSpannerClient;
  private DatabaseClient mockDbClient;
  private DatabaseAdminClient mockDbAdminClient;
  private DatabaseClientTransactionManager mockTxnManager;
  private ExecutorService executorService;

  private FluxSink mockSink;
  private AsyncResultSet mockResultSet;


  private DatabaseClientReactiveAdapter adapter;

  @BeforeEach
  void setup() {
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

    when(this.mockSpannerClient.getDatabaseClient(any())).thenReturn(this.mockDbClient);
    when(this.mockSpannerClient.getDatabaseAdminClient()).thenReturn(this.mockDbAdminClient);

    this.adapter = new DatabaseClientReactiveAdapter(this.mockSpannerClient, this.config);
    this.adapter.setTxnManager(this.mockTxnManager);

    when(this.mockTxnManager.commitTransaction()).thenReturn(ApiFutures.immediateFuture(null));

    this.mockSink =  mock(FluxSink.class);
    this.mockResultSet = mock(AsyncResultSet.class);
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

    DatabaseClientReactiveAdapter.ResultSetReadyCallback cb =
        new ResultSetReadyCallback(this.mockSink);
    CallbackResponse response = cb.cursorReady(this.mockResultSet);

    assertThat(response).isSameAs(CallbackResponse.DONE);
    verify(this.mockSink).complete();
    verifyNoMoreInteractions(this.mockSink);
  }

  @Test
  void resultSetReadyCallbackEmitsOnOk() {
    when(this.mockResultSet.tryNext()).thenReturn(CursorState.OK);
    Struct struct = Struct.newBuilder().add(Value.string("some result")).build();
    when(this.mockResultSet.getCurrentRowAsStruct()).thenReturn(struct);

    DatabaseClientReactiveAdapter.ResultSetReadyCallback cb =
        new ResultSetReadyCallback(this.mockSink);
    CallbackResponse response = cb.cursorReady(this.mockResultSet);

    assertThat(response).isSameAs(CallbackResponse.CONTINUE);
    ArgumentCaptor<SpannerClientLibraryRow> arg =
        ArgumentCaptor.forClass(SpannerClientLibraryRow.class);
    verify(this.mockSink).next(arg.capture());
    assertThat(arg.getValue().get(1)).isEqualTo("some result");

    verifyNoMoreInteractions(this.mockSink);
  }

  @Test
  void resultSetReadyCallbackWaitsOnNotReady() {
    when(this.mockResultSet.tryNext()).thenReturn(CursorState.NOT_READY);

    DatabaseClientReactiveAdapter.ResultSetReadyCallback cb =
        new ResultSetReadyCallback(this.mockSink);
    CallbackResponse response = cb.cursorReady(this.mockResultSet);

    assertThat(response).isSameAs(CallbackResponse.CONTINUE);
    verifyNoMoreInteractions(this.mockSink);
  }

  @Test
  void resultSetReadyCallbackSendsErrorOnException() {
    Exception exception = new RuntimeException("boom");
    when(this.mockResultSet.tryNext()).thenThrow(exception);

    DatabaseClientReactiveAdapter.ResultSetReadyCallback cb =
        new ResultSetReadyCallback(this.mockSink);
    CallbackResponse response = cb.cursorReady(this.mockResultSet);

    assertThat(response).isSameAs(CallbackResponse.DONE);
    verify(this.mockSink).error(exception);
    verifyNoMoreInteractions(this.mockSink);
  }
}
