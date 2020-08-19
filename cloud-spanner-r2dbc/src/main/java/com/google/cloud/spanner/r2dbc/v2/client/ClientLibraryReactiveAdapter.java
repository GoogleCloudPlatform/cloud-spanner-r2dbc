package com.google.cloud.spanner.r2dbc.v2.client;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.spanner.AsyncRunner;
import com.google.cloud.spanner.AsyncTransactionManager;
import com.google.cloud.spanner.AsyncTransactionManager.AsyncTransactionStep;
import com.google.cloud.spanner.AsyncTransactionManager.TransactionContextFuture;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Statement;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

/** Converts between R2DBC and client library transactional concepts.
 * Encapsulates useful state. */
public class ClientLibraryReactiveAdapter {

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  private final DatabaseClient dbClient;

  private AsyncTransactionManager transactionManager;

  private ExecutorService executorService;

  private TransactionContextFuture currentTransactionFuture;

  private AsyncTransactionStep<?,Long> asyncTransactionLastStep;

  public ClientLibraryReactiveAdapter(DatabaseClient dbClient, ExecutorService executorService) {
    this.dbClient = dbClient;
    this.executorService = executorService;
  }

  private boolean isInTransaction() {
    return this.currentTransactionFuture != null;
  }

  public Publisher<Void> beginTransaction() {
    return convertFutureToMono(() -> {
      logger.info("  STARTING TRANSACTION");
      this.transactionManager = dbClient.transactionManagerAsync();
      this.currentTransactionFuture = this.transactionManager.beginAsync();
      return this.currentTransactionFuture;
    }, executorService).then();
  }

  // TODO: spanner allows read queries within the transaction. Right now, only update queries get passed here
  public synchronized AsyncTransactionStep chainStatement(Statement statement) {

    logger.info("  CHAINING STEP: " + statement.getSql());
    logger.info("    " + (this.asyncTransactionLastStep == null ? "no last step" : "last step exists"));
      // The first statement in a transaction has no input, hence Void input type.
      // The subsequent statements take the previous statements' return (affected row count) as input.
      this.asyncTransactionLastStep = this.asyncTransactionLastStep == null ?
          this.currentTransactionFuture.<Long>then((ctx, aVoid) -> ctx.executeUpdateAsync(statement), this.executorService) :
          this.asyncTransactionLastStep.<Long>then((ctx, previousRowCount) -> ctx.executeUpdateAsync(statement), this.executorService);

    return this.asyncTransactionLastStep;
  }

  public Publisher<Void> commitTransaction() {

    return convertFutureToMono(() -> {
      logger.info("  COMMITTING");
      if (this.asyncTransactionLastStep == null) {
        // TODO: replace by a better non-retryable; consider not throwing at all and no-oping with warning.
        throw new RuntimeException("Nothing was executed in this transaction");
      }
      return this.asyncTransactionLastStep.commitAsync();

    }, executorService).doFinally(unusedSignal -> {
      logger.info("  closing transaction manager");
      this.transactionManager.close();
    }).then();

  }

  public Publisher<Void> rollback() {
    return convertFutureToMono(() -> {
      logger.info("  ROLLING BACK");
      if (this.asyncTransactionLastStep == null) {
        // TODO: replace by a better non-retryable; consider not throwing at all and no-oping with warning.
        throw new RuntimeException("Nothing was executed in this transaction -- nothing to roll back");
      }
      return this.transactionManager.rollbackAsync();
    }, this.executorService);
  }

  public Mono<Void> close() {
    return Mono.<Void>fromSupplier(() -> {
      this.transactionManager.close();
      return null;
    });
  }

  public Mono<Long> runDmlStatement(com.google.cloud.spanner.Statement statement) {

    return convertFutureToMono(() -> {
      if (this.isInTransaction()) {
        logger.info("   IN TRANSACTION");
        AsyncTransactionStep<?, Long> step = this.chainStatement(statement);
        return step;
      } else {
        logger.info("   NO TRANSACTION");
        // TODO: deduplicate with if-block.
        AsyncRunner runner = this.dbClient.runAsync();
        ApiFuture<Long> updateCount = runner.runAsync(
            txn -> txn.executeUpdateAsync(statement),
            this.executorService);
        return updateCount;
      }
    }, this.executorService);
  }


  private <T> Mono<T> convertFutureToMono(Supplier<ApiFuture<T>> futureSupplier, BiConsumer<MonoSink, T> successCallback) {

    return Mono.create(sink -> {
      ApiFuture future = futureSupplier.get();
      sink.onCancel(() -> future.cancel(true));

      ApiFutures.addCallback(future,
          new ApiFutureCallback<T>() {
            @Override
            public void onFailure(Throwable t) {
              sink.error(t);
            }

            @Override
            public void onSuccess(T result) {
              successCallback.accept(sink, result);
            }
          }, this.executorService);
    });

  }

  private <T> Mono<T> convertFutureToMono(Supplier<ApiFuture<T>> future, ExecutorService executorService) {
    return convertFutureToMono(future, (sink, result) -> { sink.success(result); });
  }

}
