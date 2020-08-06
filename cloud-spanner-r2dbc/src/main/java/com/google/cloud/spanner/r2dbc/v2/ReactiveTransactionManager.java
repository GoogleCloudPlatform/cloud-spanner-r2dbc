package com.google.cloud.spanner.r2dbc.v2;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.AsyncTransactionManager;
import com.google.cloud.spanner.AsyncTransactionManager.AsyncTransactionFunction;
import com.google.cloud.spanner.AsyncTransactionManager.AsyncTransactionStep;
import com.google.cloud.spanner.AsyncTransactionManager.CommitTimestampFuture;
import com.google.cloud.spanner.AsyncTransactionManager.TransactionContextFuture;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

/** Converts between R2DBC and client library transactional concepts.
 * Encapsulates useful state. */
public class ReactiveTransactionManager {
  private AsyncTransactionManager transactionManager;

  private ExecutorService executorService;

  private TransactionContextFuture currentTransactionFuture;

  private AsyncTransactionStep<?,?> asyncTransactionLastStep;

  // TODO: transaction managers are not reusable
  public ReactiveTransactionManager(AsyncTransactionManager transactionManager, ExecutorService executorService) {
    this.transactionManager = transactionManager;
    this.executorService = executorService;
  }

  public boolean isInTransaction() {
    return this.currentTransactionFuture != null;
  }

  public Publisher<Void> beginTransaction() {

    return Mono.create(sink -> {
      this.currentTransactionFuture = this.transactionManager.beginAsync();
      ApiFutures.addCallback(this.currentTransactionFuture,
          new ApiFutureCallback<TransactionContext>() {
            @Override
            public void onFailure(Throwable t) {
              sink.error(t);
            }

            @Override
            public void onSuccess(TransactionContext ctx) {
              // TODO: do we need to save unwrapped transaction context for anything?
              sink.success();
            }
          }, this.executorService);
    });
  }

  // TODO: spanner allows read queries within the transaction. Right now, only update queries get passed here
  public synchronized void chainStatement(Statement statement) {
    if (this.asyncTransactionLastStep == null) {
      this.asyncTransactionLastStep = this.currentTransactionFuture.then(
          (TransactionContext transactionContext, Void aVoid) -> {
            return transactionContext.executeUpdateAsync(statement);
          }
    , this.executorService);
    }
  }

  public Publisher<Void> commitTransaction() {

    // TODO: make a converter util for the apifuture-to-mono
    return Mono.create(sink -> {
      if (this.asyncTransactionLastStep == null) {
        // TODO: replace by a better non-retryable; consider not throwing at all and no-oping with warning.
        throw new RuntimeException("Nothing was executed in this transaction");
      }
      CommitTimestampFuture future = this.asyncTransactionLastStep.commitAsync();
      ApiFutures.addCallback(future, new ApiFutureCallback<Timestamp>() {
        @Override
        public void onFailure(Throwable t) {
          sink.error(t);
        }

        @Override
        public void onSuccess(Timestamp result) {
          // TODO: do we have a use for the commit timestamp?
          sink.success();
        }
      }, this.executorService);
    });
  }
}