package com.google.cloud.spanner.r2dbc.v2;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.r2dbc.SpannerConnectionConfiguration;
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

  /** TODO: add comment. */
  public SpannerClientLibraryConnectionFactory(SpannerConnectionConfiguration config) {
    this.config = config;

    SpannerOptions options = SpannerOptions.newBuilder().build();
            // TODO: allow customizing project ID?

    this.client = options.getService();
    this.databaseClient = this.client.getDatabaseClient(
        DatabaseId.of(config.getProjectId(), config.getInstanceName(), config.getDatabaseName()));
  }

  @Override
  public Publisher<? extends Connection> create() {
    return Mono.just(new SpannerClientLibraryConnection(this.databaseClient));
  }

  @Override
  public ConnectionFactoryMetadata getMetadata() {
    return null;
  }
}
