package com.google.cloud.spanner.r2dbc.v2;

import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.r2dbc.SpannerConnectionConfiguration;
import com.google.cloud.spanner.r2dbc.client.Client;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

/**
 * The factory is for a specific project/instance/database + credentials.
 *
 */
public class SpannerClientLibraryConnectionFactory implements ConnectionFactory {

  private SpannerConnectionConfiguration config;

  private Spanner client;
  private DatabaseClient databaseClient;
  private DatabaseAdminClient databaseAdminClient;

  // fall back to grpc for unsupported client library async functionality (DDL)
  private Client grpcClient;

  /** TODO: add comment. */
  public SpannerClientLibraryConnectionFactory(Client grpcClient, SpannerConnectionConfiguration config) {
    this.config = config;
    this.grpcClient = grpcClient;

    SpannerOptions options = SpannerOptions.newBuilder().build();
            // TODO: allow customizing project ID?

    this.client = options.getService();
    this.databaseClient = this.client.getDatabaseClient(
        DatabaseId.of(config.getProjectId(), config.getInstanceName(), config.getDatabaseName()));
    this.databaseAdminClient = this.client.getDatabaseAdminClient();
  }

  @Override
  public Publisher<? extends Connection> create() {
    return Mono.just(new SpannerClientLibraryConnection(
        this.databaseClient, this.databaseAdminClient, this.grpcClient, this.config));
  }

  @Override
  public ConnectionFactoryMetadata getMetadata() {
    return null;
  }
}