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

import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import javax.annotation.Nullable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;

/**
 * Benchmarks for DML operations: updating rows.
 */
public class DmlBenchmark extends BenchmarkState {

  /**
   * DML with r2dbc driver.
   */
  @Benchmark
  public void testDmlR2dbcDriver(R2dbcConnectionState r2dbcState, CommonState common,
      Blackhole blackhole) {

    String query = common.getSingleRowUpdateQuery();

    Integer result =
        Flux.from(r2dbcState.r2dbcConnection.createStatement(query).execute())
            .flatMap(spannerResult -> spannerResult.getRowsUpdated())
            .blockFirst();

    blackhole.consume(result);
  }


  /**
   * DML with client library.
   */
  @Benchmark
  public void testDmlClientLibrary(
      ClientLibraryConnectionState clientLibraryState, CommonState common, Blackhole blackhole) {

    final String query = common.getSingleRowUpdateQuery();

    Long result = clientLibraryState.dbClient
        .readWriteTransaction().run(new TransactionCallable<Long>() {
          @Nullable
          @Override
          public Long run(TransactionContext transactionContext) throws Exception {
            return transactionContext.executeUpdate(Statement.of(query));
          }
        });

    blackhole.consume(result);
  }

}
