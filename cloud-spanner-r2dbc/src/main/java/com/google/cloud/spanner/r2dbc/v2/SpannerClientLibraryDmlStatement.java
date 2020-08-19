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
    return this.reactiveTransactionManager.runDmlStatement(com.google.cloud.spanner.Statement.of(this.query))
      .transform(numRowsUpdatedMono -> Mono.just(new SpannerClientLibraryResult(Flux.empty(), numRowsUpdatedMono.map(this::longToInt))));
  }

  private int longToInt(Long numRows) {
    if (numRows > Integer.MAX_VALUE) {
      logger.warn("Number of updated rows exceeds maximum integer value; actual rows updated = %s; returning max int value", numRows);
      return Integer.MAX_VALUE;
    }
    return numRows.intValue();
  }

}
