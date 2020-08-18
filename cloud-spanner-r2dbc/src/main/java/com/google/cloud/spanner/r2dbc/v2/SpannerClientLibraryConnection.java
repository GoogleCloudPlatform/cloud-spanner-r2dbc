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
import reactor.core.publisher.Mono;

public class SpannerClientLibraryConnection implements Connection {

  private DatabaseClient dbClient;

  private DatabaseAdminClient dbAdminClient;

  // fall back to grpc for unsupported client library async functionality (DDL)
  private Client grpcClient;

  private SpannerConnectionConfiguration config;

  private ReactiveTransactionManager reactiveTransactionManager;

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
    this.reactiveTransactionManager = new ReactiveTransactionManager(dbClient.transactionManagerAsync(), executorService);

  }

  @Override
  public Publisher<Void> beginTransaction() {


    return this.reactiveTransactionManager.beginTransaction();
  }

  @Override
  public Publisher<Void> close() {
    // TODO (elfel): VERY IMPORTANT -- close client library transaction manager
    // Close the executor service.
    // TODO: manage the one in SpannerClientLibraryDdlStatement, too
    this.executorService.shutdown();
    // TODO: listen to shutdown?
    return Mono.just(null);
  }

  @Override
  public Publisher<Void> commitTransaction() {
    // TODO (elfel): there will be trouble. Statement is created outside of subscription flow, so:
    // create statement 1 -> create statement 2 -> begin txn -> execute statement 1 ->
    // commit txn -> begin txn -> execute statement 2 !!!! now statement 2 still has the transaction from the second step.
    Publisher<Void> result = this.reactiveTransactionManager.commitTransaction();
    this.reactiveTransactionManager = new ReactiveTransactionManager(dbClient.transactionManagerAsync(), executorService);
    return result;
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
      System.out.println("DDL statement detected: " + query);
      return new SpannerClientLibraryDdlStatement(query, this.grpcClient, this.config);
    } else if (type == StatementType.DML) {
      System.out.println("DML statement detected: " + query);
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
    this.reactiveTransactionManager = new ReactiveTransactionManager(dbClient.transactionManagerAsync(), executorService);
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
