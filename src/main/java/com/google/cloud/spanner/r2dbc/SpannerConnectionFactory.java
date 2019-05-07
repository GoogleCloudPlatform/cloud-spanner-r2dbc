/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spanner.r2dbc;

import com.google.cloud.spanner.r2dbc.client.Client;
import com.google.cloud.spanner.r2dbc.client.GrpcClient;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import java.io.IOException;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

/**
 * An implementation of {@link ConnectionFactory} for creating connections to Cloud Spanner
 * database.
 */
public class SpannerConnectionFactory implements ConnectionFactory {

  private SpannerConnectionConfiguration config;

  private Client client = new GrpcClient();

  public SpannerConnectionFactory(SpannerConnectionConfiguration config) {
    this.config = config;
  }

  @Override
  public Publisher<SpannerConnection> create() {

    return Mono.defer(() -> {
      try {
        this.client.initialize();
        return Mono.just(new SpannerConnection(this.client, this.config));
      } catch (IOException e) {
        return Mono.error(e);
      }
    });
  }

  @Override
  public ConnectionFactoryMetadata getMetadata() {
    return SpannerConnectionFactoryMetadata.INSTANCE;
  }

  public void setClient(Client client) {
    this.client = client;
  }

}
