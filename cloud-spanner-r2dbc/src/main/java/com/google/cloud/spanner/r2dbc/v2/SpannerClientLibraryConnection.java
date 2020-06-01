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
import org.reactivestreams.Publisher;

public class SpannerClientLibraryConnection implements Connection {

  private DatabaseClient dbClient;

  private DatabaseAdminClient dbAdminClient;

  // fall back to grpc for unsupported client library async functionality (DDL)
  private Client grpcClient;

  private SpannerConnectionConfiguration config;

  public SpannerClientLibraryConnection(DatabaseClient dbClient,
      DatabaseAdminClient dbAdminClient,
      Client grpcClient,
      SpannerConnectionConfiguration config) {
    this.dbClient = dbClient;
    this.dbAdminClient = dbAdminClient;
    this.grpcClient = grpcClient;
    this.config = config;
  }

  @Override
  public Publisher<Void> beginTransaction() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Publisher<Void> close() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Publisher<Void> commitTransaction() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Batch createBatch() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Publisher<Void> createSavepoint(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Statement createStatement(String query) {
    StatementType type = StatementParser.getStatementType(query);
    if (type == StatementType.DDL) {
      System.out.println("DDL statement detected: " + query);
      return new SpannerClientLibraryDdlStatement(query, this.grpcClient, this.config);
    } else if (type == StatementType.DML) {
      System.out.println("DML statement detected: " + query);
      return new SpannerClientLibraryDmlStatement(this.dbClient, query);
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
    throw new UnsupportedOperationException();
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
