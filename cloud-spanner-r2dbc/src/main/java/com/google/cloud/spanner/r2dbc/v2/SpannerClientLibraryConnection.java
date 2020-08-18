package com.google.cloud.spanner.r2dbc.v2;

import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.r2dbc.SpannerConnectionConfiguration;
import com.google.cloud.spanner.r2dbc.client.Client;
import com.google.cloud.spanner.r2dbc.statement.StatementParser;
import com.google.cloud.spanner.r2dbc.statement.StatementType;
import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionMetadata;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.ValidationDepth;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class SpannerClientLibraryConnection implements Connection {

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  private DatabaseClient dbClient;

  private DatabaseAdminClient dbAdminClient;

  // fall back to grpc for unsupported client library async functionality (DDL)
  private Client grpcClient;

  private SpannerConnectionConfiguration config;

  private final ReactiveTransactionManager reactiveTransactionManager;

  // TODO: make thread pool customizable
  private ExecutorService executorService = Executors.newFixedThreadPool(4);

  public SpannerClientLibraryConnection(DatabaseClient dbClient,
      DatabaseAdminClient dbAdminClient,
      Client grpcClient,
      SpannerConnectionConfiguration config) {
    this.dbClient = dbClient;
    this.dbAdminClient = dbAdminClient;
    this.grpcClient = grpcClient;
    this.config = config;
    this.reactiveTransactionManager = new ReactiveTransactionManager(dbClient, executorService);

  }

  @Override
  public Publisher<Void> beginTransaction() {


    return this.reactiveTransactionManager.beginTransaction();
  }

  @Override
  public Publisher<Void> close() {
    return this.reactiveTransactionManager.close()
        .then(Mono.fromSupplier(() -> {
          logger.info("  shutting down executor service");
          this.executorService.shutdown();
          return null;
        }));
  }

  @Override
  public Publisher<Void> commitTransaction() {
    return this.reactiveTransactionManager.commitTransaction();
  }

  @Override
  public Batch createBatch() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Publisher<Void> createSavepoint(String name) {
    throw new UnsupportedOperationException();
  }

  // TODO: test whether select statements interspersed with update statements need to be handled as part of async flow
  @Override
  public Statement createStatement(String query) {
    StatementType type = StatementParser.getStatementType(query);
    if (type == StatementType.DDL) {
      logger.info("DDL statement detected: " + query);
      return new SpannerClientLibraryDdlStatement(query, this.grpcClient, this.config);
    } else if (type == StatementType.DML) {
      logger.info("DML statement detected: " + query);
      return new SpannerClientLibraryDmlStatement(this.dbClient, this.reactiveTransactionManager, query);
    }
    return new SpannerClientLibraryStatement(this.dbClient, query);
  }

  @Override
  public boolean isAutoCommit() {
    return false;
  }

  @Override
  public ConnectionMetadata getMetadata() {
    throw new UnsupportedOperationException();
  }

  @Override
  public IsolationLevel getTransactionIsolationLevel() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Publisher<Void> releaseSavepoint(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Publisher<Void> rollbackTransaction() {
    Publisher<Void> result = this.reactiveTransactionManager.rollback();
    return result;
  }

  @Override
  public Publisher<Void> rollbackTransactionToSavepoint(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Publisher<Void> setAutoCommit(boolean autoCommit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Publisher<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Publisher<Boolean> validate(ValidationDepth depth) {
    throw new UnsupportedOperationException();
  }
}
