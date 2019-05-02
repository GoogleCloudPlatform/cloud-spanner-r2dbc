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

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionManager;
import com.google.cloud.spanner.TransactionManager.TransactionState;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;

/**
 * Test for {@link SpannerConnection}.
 */
public class SpannerConnectionTest {

  private DatabaseClient databaseClient;
  private TransactionManager transactionManager;
  private TransactionContext transactionContext;

  /** Initializes the mocks for the test. */
  @Before
  public void setupMocks() {
    this.databaseClient = Mockito.mock(DatabaseClient.class);
    this.transactionManager = Mockito.mock(TransactionManager.class);
    this.transactionContext = Mockito.mock(TransactionContext.class);

    when(this.databaseClient.transactionManager()).thenReturn(this.transactionManager);
    when(this.transactionManager.begin()).thenReturn(this.transactionContext);
  }

  @Test
  public void testBeginTransactionTwiceNoop() {
    SpannerConnection connection = new SpannerConnection(databaseClient);

    Mono.from(connection.beginTransaction()).block();
    verify(this.transactionManager, times(1)).begin();

    // Repeated begin() is a no-op.
    when(this.transactionManager.getState()).thenReturn(TransactionState.STARTED);
    Mono.from(connection.beginTransaction()).block();
    verify(this.transactionManager, times(1)).begin();
  }

  @Test
  public void testBeginTransactionAfterCommit() {
    SpannerConnection connection = new SpannerConnection(databaseClient);

    Mono.from(connection.beginTransaction()).block();
    when(this.transactionManager.getState()).thenReturn(TransactionState.STARTED);
    verify(this.transactionManager, times(1)).begin();

    Mono.from(connection.commitTransaction()).block();
    when(this.transactionManager.getState()).thenReturn(TransactionState.COMMITTED);

    Mono.from(connection.beginTransaction()).block();
    verify(this.transactionManager, times(2)).begin();
  }
}
