package com.google.cloud.spanner.r2dbc.v2;

import static com.google.cloud.spanner.r2dbc.util.ApiFutureUtil.convertFutureToMono;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.AsyncTransactionManager;
import com.google.cloud.spanner.AsyncTransactionManager.AsyncTransactionFunction;
import com.google.cloud.spanner.AsyncTransactionManager.AsyncTransactionStep;
import com.google.cloud.spanner.AsyncTransactionManager.CommitTimestampFuture;
import com.google.cloud.spanner.AsyncTransactionManager.TransactionContextFuture;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.r2dbc.util.ApiFutureUtil;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

/** Converts between R2DBC and client library transactional concepts.
 * Encapsulates useful state. */
public class ReactiveTransactionManager {

  private final DatabaseClient dbClient;

  private AsyncTransactionManager transactionManager;

  private ExecutorService executorService;

  private TransactionContextFuture currentTransactionFuture;

  private AsyncTransactionStep<?,Long> asyncTransactionLastStep;

  public ReactiveTransactionManager(DatabaseClient dbClient, ExecutorService executorService) {
    this.dbClient = dbClient;
    this.executorService = executorService;
  }

  public boolean isInTransaction() {
    return this.currentTransactionFuture != null;
  }

  public Publisher<Void> beginTransaction() {

    return Mono.create(sink -> {
      this.transactionManager = dbClient.transactionManagerAsync();
      this.currentTransactionFuture = this.transactionManager.beginAsync();
      convertFutureToMono(sink, this.currentTransactionFuture, executorService);
    });
  }

  // TODO: spanner allows read queries within the transaction. Right now, only update queries get passed here
  public synchronized AsyncTransactionStep chainStatement(Statement statement) {

      // The first statement in a transaction has no input, hence Void input type.
      // The subsequent statements take the previous statements' return (affected row count) as input.
      this.asyncTransactionLastStep = this.asyncTransactionLastStep == null ?
          this.currentTransactionFuture.<Long>then((ctx, aVoid) -> ctx.executeUpdateAsync(statement), this.executorService) :
          this.asyncTransactionLastStep.<Long>then((ctx, previousRowCount) -> ctx.executeUpdateAsync(statement), this.executorService);

    return this.asyncTransactionLastStep;
  }

  public Publisher<Void> commitTransaction() {

    return Mono.<Void>create(sink -> {
      if (this.asyncTransactionLastStep == null) {
        // TODO: replace by a better non-retryable; consider not throwing at all and no-oping with warning.
        throw new RuntimeException("Nothing was executed in this transaction");
      }
      CommitTimestampFuture future = this.asyncTransactionLastStep.commitAsync();
      convertFutureToMono(sink, future, executorService);
    }).doFinally(unusedSignal -> {
      this.transactionManager.close();
    });


  }

  public Publisher<Void> rollback() {

    return Mono.<Void>create(sink -> {
      if (this.asyncTransactionLastStep == null) {
        // TODO: replace by a better non-retryable; consider not throwing at all and no-oping with warning.
        throw new RuntimeException("Nothing was executed in this transaction -- nothing to roll back");
      }
      ApiFuture<Void> future = this.transactionManager.rollbackAsync();
      convertFutureToMono(sink, future, executorService);
    }).doFinally(unusedSignal -> {
      this.transactionManager.close();
    });
  }

  public Mono<Void> close() {
    return Mono.<Void>fromSupplier(() -> {
      this.transactionManager.close();
      return null;
    });
  }
}
