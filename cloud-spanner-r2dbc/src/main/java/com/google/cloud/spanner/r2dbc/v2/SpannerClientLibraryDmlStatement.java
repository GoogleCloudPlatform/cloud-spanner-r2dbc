package com.google.cloud.spanner.r2dbc.v2;

import static com.google.cloud.spanner.r2dbc.util.ApiFutureUtil.convertFutureToMono;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.spanner.AsyncResultSet;
import com.google.cloud.spanner.AsyncResultSet.CallbackResponse;
import com.google.cloud.spanner.AsyncRunner;
import com.google.cloud.spanner.AsyncTransactionManager.AsyncTransactionStep;
import com.google.cloud.spanner.DatabaseClient;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

public class SpannerClientLibraryDmlStatement implements Statement {

  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  
  // YOLO; very temporary. TODO: use global one in SpannerClientLibraryConnection.
  private ExecutorService executorService = Executors.newSingleThreadExecutor();

  private DatabaseClient databaseClient;

  private ReactiveTransactionManager reactiveTransactionManager;

  private String query;

  // TODO: accept a transaction
  public SpannerClientLibraryDmlStatement(DatabaseClient databaseClient, ReactiveTransactionManager reactiveTransactionManager, String query) {
    this.databaseClient = databaseClient;
    this.reactiveTransactionManager = reactiveTransactionManager;
    this.query = query;
  }

  @Override
  public Statement add() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Statement bind(int index, Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Statement bind(String name, Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Statement bindNull(int index, Class<?> type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Statement bindNull(String name, Class<?> type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Publisher<? extends Result> execute() {

    return Mono.<Integer>create(sink -> this.executeToMono(sink))
        .transform(numRowsUpdatedMono -> Mono.just(new SpannerClientLibraryResult(Flux.empty(), numRowsUpdatedMono)));
  }


  private void executeToMono(MonoSink sink) {
    if (reactiveTransactionManager.isInTransaction()) {
      logger.info("   IN TRANSACTION");
      // TODO: It's Long return type now because only update statements are supported for now. Test SELECT.
      AsyncTransactionStep<?, Long> step = reactiveTransactionManager.chainStatement(com.google.cloud.spanner.Statement.of(this.query));

      sink.onCancel(() -> step.cancel(true));
      convertFutureToMono(sink, step, executorService, (result) -> {
        if (result > Integer.MAX_VALUE) {
          // TODO: implement custom unretryable exception class.
          sink.error(
              new RuntimeException("Number of updated rows exceeds maximum integer value"));
        } else {
          sink.success(result.intValue());
        }
      });

    } else {
      logger.info("   NO TRANSACTION");
      // TODO: deduplicate with if-block.
      AsyncRunner runner = this.databaseClient.runAsync();
      ApiFuture<Long> updateCount = runner.runAsync(
          txn -> txn.executeUpdateAsync(com.google.cloud.spanner.Statement.of(this.query)),
          this.executorService);

      sink.onCancel(() -> updateCount.cancel(true));

      convertFutureToMono(sink, updateCount, executorService, (result) -> {
        if (result > Integer.MAX_VALUE) {
          // TODO: implement custom unretryable exception class.
          sink.error(
              new RuntimeException("Number of updated rows exceeds maximum integer value"));
        } else {
          sink.success(result.intValue());
        }
      });
    }
  }

}
