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

package com.google.cloud.spanner.r2dbc.benchmarks;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;

/**
 * Benchmarks for DDL operations -- creating and dropping a table.
 */
public class DdlBenchmark extends BenchmarkState {

  /**
   * DDL with r2dbc driver.
   */
  @Benchmark
  @OutputTimeUnit(TimeUnit.SECONDS)
  public void testDdlR2dbcDriver(R2dbcConnectionState r2dbcState, CommonState common,
      Blackhole blackhole) {

    Integer suffix = common.getRandomValue();
    String createQuery = common.getCreateTableQuery(suffix);
    String dropQuery = common.getDropTableQuery(suffix);


    Flux.from(r2dbcState.r2dbcConnection.createStatement(createQuery).execute())
        .thenMany(Flux.from(r2dbcState.r2dbcConnection.createStatement(dropQuery).execute()))
        .blockLast();
  }

  /**
   * DDL with client library.
   */
  @Benchmark
  @OutputTimeUnit(TimeUnit.SECONDS)
  public void testDdlClientLibrary(
      ClientLibraryConnectionState clientLibraryState, CommonState common, Blackhole blackhole)
      throws Exception {

    Integer suffix = common.getRandomValue();
    String createQuery = common.getCreateTableQuery(suffix);
    String dropQuery = common.getDropTableQuery(suffix);

    clientLibraryState.dbAdminClient
        .updateDatabaseDdl(TEST_INSTANCE, TEST_DATABASE, Arrays.asList(createQuery), null)
        .get();

    clientLibraryState.dbAdminClient
        .updateDatabaseDdl(TEST_INSTANCE, TEST_DATABASE, Arrays.asList(dropQuery), null)
        .get();

  }

}
