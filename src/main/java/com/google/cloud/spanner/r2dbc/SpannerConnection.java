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

import com.google.cloud.spanner.r2dbc.client.Client;
import com.google.protobuf.ByteString;
import com.google.spanner.v1.Session;
import com.google.spanner.v1.Transaction;
import com.google.spanner.v1.TransactionOptions;
import com.google.spanner.v1.TransactionOptions.ReadWrite;
import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.IsolationLevel;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * {@link Connection} implementation for Cloud Spanner.
 */
public class SpannerConnection implements Connection {

  private static final TransactionOptions READ_WRITE_TRANSACTION =
      TransactionOptions.newBuilder()
          .setReadWrite(ReadWrite.getDefaultInstance())
          .build();

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  private final Client client;

  private final SpannerConnectionConfiguration config;

  private volatile SpannerStatementExecutionContext ctx;

  /**
   * Instantiates a Spanner session with given configuration.
   * @param client client controlling low-level Spanner operations
   * @param session Spanner session to use for all interactions on this connection.
   */
  public SpannerConnection(Client client, Session session, SpannerConnectionConfiguration config) {
    this.client = client;
    this.ctx = new SpannerStatementExecutionContext(session);
    this.config = config;
  }

  @Override
  public Publisher<Void> beginTransaction() {
    return this.beginTransaction(READ_WRITE_TRANSACTION);
  }

  /**
   * Begins a new transaction with the specified {@link TransactionOptions}.
   */
  public Mono<Void> beginTransaction(TransactionOptions transactionOptions) {
    return this.client.beginTransaction(this.ctx.getSessionName(), transactionOptions)
        .doOnNext(
            transaction -> this.ctx.setTransaction(transaction, transactionOptions))
        .then();
  }

  @Override
  public Mono<Void> commitTransaction() {
    return commitTransaction(true);
  }

  private Mono<Void> commitTransaction(boolean logMessage) {
    return Mono.defer(() -> {
      if (this.ctx.getTransactionId() != null && this.ctx.isReadWrite()) {
        return this.client.commitTransaction(
            this.ctx.getSessionName(), this.ctx.getTransaction())
            .doOnNext(response -> this.ctx.setTransaction(null, null))
            .then();
      }

      if (logMessage) {
        if (this.ctx.getTransactionId() == null) {
          this.logger.debug("commitTransaction() is a no-op; called with no transaction active.");
        } else if (!this.ctx.isReadWrite()) {
          this.logger.debug("commitTransaction() is a no-op; "
              + "called outside of a read-write transaction.");
        }
      }
      return Mono.empty();
    });
  }

  @Override
  public Mono<Void> rollbackTransaction() {
    return Mono.defer(() -> {
      if (this.ctx.getTransactionId() == null) {
        this.logger.warn("rollbackTransaction() is a no-op; called with no transaction active.");
        return Mono.empty();
      }

      return this.client
            .rollbackTransaction(this.ctx.getSessionName(), this.ctx.getTransaction())
            .doOnSuccess(response -> this.ctx.setTransaction(null, null));
    });
  }

  @Override
  public Mono<Void> close() {
    return commitTransaction(false).then(this.client.deleteSession(this.ctx.getSessionName()));
  }

  @Override
  public Batch createBatch() {
    return null;
  }

  @Override
  public Publisher<Void> createSavepoint(String s) {
    return null;
  }

  @Override
  public SpannerStatement createStatement(String sql) {
    SpannerStatement statement
        = new SpannerStatement(this.client, this.ctx, sql, this.config);

    return statement;
  }

  @Override
  public Publisher<Void> releaseSavepoint(String s) {
    return null;
  }

  @Override
  public Publisher<Void> rollbackTransactionToSavepoint(String s) {
    return null;
  }

  @Override
  public Publisher<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
    return null;
  }

  /**
   * Returns the {@link SpannerStatementExecutionContext} associated with the current
   * {@link Connection}.
   * The context is aware of the current session and optional transaction. It also provides
   * monotonically increasing {@codde seqNo} used for multiple DML statements within a transaction.
   * @return execution context
   */
  public SpannerStatementExecutionContext getExecutionContext() {
    return this.ctx;
  }


  /**
   * A class to hold session and transaction-related data that needs to be communicated from
   * {@link SpannerConnection} to {@link SpannerStatement}.
   */
  public static class SpannerStatementExecutionContext implements StatementExecutionContext {

    private Session session;

    private Transaction transaction;

    private TransactionOptions transactionOptions;

    private AtomicLong seqNum = new AtomicLong(0);

    /**
     * Creates a new transaction with a given session.
     * Sessions are immutable in the execution context.
     * @param session the session under which the current context is used.
     */
    private SpannerStatementExecutionContext(Session session) {
      this.session = session;
    }

    /**
     * Sets a new transaction or unsets the current one if {@code null} is passed in.
     * Transactions are mutable in the execution context.
     * @param transaction the newly opened transaction
     */
    private void setTransaction(
        @Nullable Transaction transaction, @Nullable TransactionOptions transactionOptions) {
      this.transaction = transaction;
      this.transactionOptions = transactionOptions;
    }

    public ByteString getTransactionId() {
      return this.transaction == null ? null : this.transaction.getId();
    }

    public Transaction getTransaction() {
      return this.transaction;
    }

    public String getSessionName() {
      return this.session == null ? null : this.session.getName();
    }

    public long nextSeqNum() {
      return this.seqNum.getAndIncrement();
    }

    public boolean isReadWrite() {
      return this.transactionOptions.hasReadWrite();
    }

    public boolean isPartitionedDml() {
      return this.transactionOptions.hasPartitionedDml();
    }

  }
}
