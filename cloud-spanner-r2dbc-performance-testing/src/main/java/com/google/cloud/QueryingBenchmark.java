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

package com.google.cloud;

import static com.google.cloud.spanner.r2dbc.SpannerConnectionFactoryProvider.DRIVER_NAME;
import static com.google.cloud.spanner.r2dbc.SpannerConnectionFactoryProvider.INSTANCE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import java.util.List;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Benchmarks for Cloud Spanner R2DBC driver.
 */
public class QueryingBenchmark {
  private static final String TEST_INSTANCE
      = System.getProperty("spanner.instance", "reactivetest");
  private static final String TEST_DATABASE
      = System.getProperty("spanner.database", "met");
  private static final String TEST_PROJECT
      = System.getProperty("gcp.project", "elfel-spring");
  private static final String QUERY
      = "SELECT * FROM met_objects LIMIT ";

  /**
   * R2DBC shared connection.
   */
  @State(Scope.Benchmark)
  public static class R2dbcConnectionState {
    final Connection r2dbcConnection;

    /** come on checkstyle. */
    public R2dbcConnectionState() {
      ConnectionFactory connectionFactory = ConnectionFactories.get(
          ConnectionFactoryOptions.builder()
              .option(Option.valueOf("project"), TEST_PROJECT)
              .option(DRIVER, DRIVER_NAME)
              .option(INSTANCE, TEST_INSTANCE)
              .option(DATABASE, TEST_DATABASE)
              .build());

      this.r2dbcConnection = Mono.from(connectionFactory.create()).block();
    }
  }

  /**
   * querying.
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public void testMethod(R2dbcConnectionState r2dbcState, Blackhole blackhole) {

    int numRows = 10;

    List<String> result =
        Flux.from(r2dbcState.r2dbcConnection.createStatement(QUERY + numRows).execute())
        .flatMap(spannerResult -> spannerResult.map(
            (r, meta) -> r.get("title", String.class)
        )).collectList()
        .block();

    blackhole.consume(result);

  }


}
