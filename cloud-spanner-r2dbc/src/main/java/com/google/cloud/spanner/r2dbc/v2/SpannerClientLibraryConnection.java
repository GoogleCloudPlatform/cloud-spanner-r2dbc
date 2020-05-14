package com.google.cloud.spanner.r2dbc.v2;

import com.google.cloud.spanner.DatabaseClient;
import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionMetadata;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.ValidationDepth;
import org.reactivestreams.Publisher;

public class SpannerClientLibraryConnection implements Connection {

  private DatabaseClient databaseClient;

  public SpannerClientLibraryConnection(DatabaseClient databaseClient) {
    this.databaseClient = databaseClient;
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
  public Statement createStatement(String sql) {
    return new SpannerClientLibraryStatement(this.databaseClient, sql);
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
