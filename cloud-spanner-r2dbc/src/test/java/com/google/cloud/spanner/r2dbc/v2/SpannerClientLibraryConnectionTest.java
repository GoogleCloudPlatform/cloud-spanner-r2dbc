/*
 * Copyright 2020-2020 Google LLC
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

import static io.r2dbc.spi.IsolationLevel.READ_COMMITTED;
import static io.r2dbc.spi.IsolationLevel.READ_UNCOMMITTED;
import static io.r2dbc.spi.IsolationLevel.REPEATABLE_READ;
import static io.r2dbc.spi.IsolationLevel.SERIALIZABLE;
import static io.r2dbc.spi.TransactionDefinition.ISOLATION_LEVEL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;
import io.r2dbc.spi.Batch;
import io.r2dbc.spi.TransactionDefinition;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class SpannerClientLibraryConnectionTest {

  DatabaseClientReactiveAdapter mockAdapter;

  SpannerClientLibraryConnection connection;

  /** Sets up mocks. */
  @BeforeEach
  public void setUpMocks() {
    this.mockAdapter = mock(DatabaseClientReactiveAdapter.class);
    this.connection = new SpannerClientLibraryConnection(this.mockAdapter);

    when(this.mockAdapter.beginReadonlyTransaction(any())).thenReturn(Mono.empty());
    when(this.mockAdapter.beginTransaction()).thenReturn(Mono.empty());
  }

  @Test
  void beginReadonlyTransactionUsesStrongConsistencyByDefault() {
    StepVerifier.create(this.connection.beginReadonlyTransaction())
        .verifyComplete();

    verify(this.mockAdapter).beginReadonlyTransaction(TimestampBound.strong());
  }

  @Test
  void shouldBeginTransactionInReadOnlyMode() {
    SpannerTransactionDefinition readOnlyDefinition = new SpannerTransactionDefinition.Builder()
        .with(TransactionDefinition.READ_ONLY, true)
        .build();

    StepVerifier.create(this.connection.beginTransaction(readOnlyDefinition))
        .verifyComplete();

    verify(this.mockAdapter).beginReadonlyTransaction(TimestampBound.strong());
  }

  @Test
  void shouldBeginTransactionInReadWriteMode() {
    SpannerTransactionDefinition readWriteDefinition = new SpannerTransactionDefinition.Builder()
        .with(TransactionDefinition.READ_ONLY, false)
        .build();

    StepVerifier.create(this.connection.beginTransaction(readWriteDefinition))
        .verifyComplete();

    verify(this.mockAdapter).beginTransaction();
  }

  @Test
  void shouldBeginTransactionInReadWriteModeByDefault() {
    SpannerTransactionDefinition readWriteDefinition = new SpannerTransactionDefinition.Builder()
        .build();   // absence of attribute indicates read write transaction

    StepVerifier.create(this.connection.beginTransaction(readWriteDefinition))
        .verifyComplete();

    verify(this.mockAdapter).beginTransaction();
  }

  @Test
  void shouldBeginTransactionWithGivenTimestampBound() {
    TimestampBound fiveSecondTimestampBound = TimestampBound.ofExactStaleness(5L, TimeUnit.SECONDS);

    SpannerTransactionDefinition staleTransactionDefinition =
        new SpannerTransactionDefinition.Builder()
            .with(TransactionDefinition.READ_ONLY, true)
            .with(SpannerConstants.TIMESTAMP_BOUND, fiveSecondTimestampBound)
            .build();

    StepVerifier.create(this.connection.beginTransaction(staleTransactionDefinition))
        .verifyComplete();

    verify(this.mockAdapter).beginReadonlyTransaction(fiveSecondTimestampBound);
  }

  @Test
  void shouldThrowErrorWhenBeginTransactionWithOtherThanDefaultOrSerializable() {
    SpannerTransactionDefinition.Builder builder = new SpannerTransactionDefinition.Builder();

    // default isolation
    SpannerTransactionDefinition defaultIsolation = builder.with(ISOLATION_LEVEL, null)
        .build();
    StepVerifier.create(this.connection.beginTransaction(defaultIsolation))
        .verifyComplete();

    // SERIALIZABLE isolation
    SpannerTransactionDefinition serializable = builder.with(ISOLATION_LEVEL, SERIALIZABLE)
        .build();
    StepVerifier.create(this.connection.beginTransaction(serializable))
        .verifyComplete();

    // READ_COMMITTED isolation
    SpannerTransactionDefinition readCommitted = builder.with(ISOLATION_LEVEL, READ_COMMITTED)
        .build();
    StepVerifier.create(this.connection.beginTransaction(readCommitted))
        .expectError(UnsupportedOperationException.class)
        .verify();

    // READ_UNCOMMITTED isolation
    SpannerTransactionDefinition readUncommitted = builder.with(ISOLATION_LEVEL, READ_UNCOMMITTED)
        .build();
    StepVerifier.create(this.connection.beginTransaction(readUncommitted))
        .expectError(UnsupportedOperationException.class)
        .verify();

    // REPEATABLE_READ isolation
    SpannerTransactionDefinition repeatableRead = builder.with(ISOLATION_LEVEL, REPEATABLE_READ)
        .build();
    StepVerifier.create(this.connection.beginTransaction(repeatableRead))
        .expectError(UnsupportedOperationException.class)
        .verify();
  }

  @Test
  void shouldGetAndSetSerializationOnlyAsIsolationLevel() {
    StepVerifier.create(this.connection.setTransactionIsolationLevel(SERIALIZABLE))
        .verifyComplete();
    assertThat(this.connection.getTransactionIsolationLevel()).isEqualTo(SERIALIZABLE);

    StepVerifier.create(this.connection.setTransactionIsolationLevel(READ_COMMITTED))
        .expectError(UnsupportedOperationException.class)
        .verify();

    StepVerifier.create(this.connection.setTransactionIsolationLevel(READ_UNCOMMITTED))
        .expectError(UnsupportedOperationException.class)
        .verify();

    StepVerifier.create(this.connection.setTransactionIsolationLevel(REPEATABLE_READ))
        .expectError(UnsupportedOperationException.class)
        .verify();

    StepVerifier.create(this.connection.setTransactionIsolationLevel(null))
        .expectError(IllegalArgumentException.class)
        .verify();
  }

  @Test
  void batchUsesCorrectAdapter() {
    Batch batch = this.connection.createBatch();
    when(this.mockAdapter.runBatchDml(anyList()))
        .thenReturn(Flux.just(
            new SpannerClientLibraryResult(Flux.empty(), 35)
        ));
    StepVerifier.create(
        Flux.from(
            batch.add("UPDATE tbl SET col1=val1").execute()
        ).flatMap(r -> r.getRowsUpdated())
    ).expectNext(35)
    .verifyComplete();

    ArgumentCaptor<List<Statement>> argCaptor = ArgumentCaptor.forClass(List.class);
    verify(this.mockAdapter).runBatchDml(argCaptor.capture());
    List<Statement> args = argCaptor.getValue();
    assertThat(args).hasSize(1);
    assertThat(args.get(0).getSql()).isEqualTo("UPDATE tbl SET col1=val1");
  }

  @Test
  void setLockWaitTimeoutNotSupported() {
    StepVerifier.create(
        this.connection.setLockWaitTimeout(Duration.ofSeconds(1))
    ).verifyError(UnsupportedOperationException.class);
  }

  @Test
  void setStatementTimeoutNotSupported() {
    StepVerifier.create(
        this.connection.setStatementTimeout(Duration.ofSeconds(1))
    ).verifyError(UnsupportedOperationException.class);
  }
}
