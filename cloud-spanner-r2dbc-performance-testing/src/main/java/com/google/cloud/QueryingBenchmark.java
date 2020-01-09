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

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;

/**
 * Benchmarks for Cloud Spanner R2DBC driver.
 */
public class QueryingBenchmark extends BenchmarkState {

  private static final String QUERY
      = "SELECT * FROM met_objects LIMIT ";

  /**
   * querying with r2dbc driver.
   */
  @Benchmark
  public List<String> testQueryingR2dbc(R2dbcConnectionState r2dbcState) {
    int numRows = 10;

    List<String> result =
        Flux.from(r2dbcState.r2dbcConnection.createStatement(QUERY + numRows).execute())
        .flatMap(spannerResult -> spannerResult.map(
            (r, meta) -> r.get("title", String.class)
        )).collectList()
        .block();

    return result;

  }

  /**
   * querying with client library.
   */
  @Benchmark
  public ResultSet testQueryingClientLibrary(
      ClientLibraryConnectionState clientLibraryState) {

    int numRows = 10;

    ResultSet resultSet = clientLibraryState.dbClient.singleUse()
        .executeQuery(Statement.of(QUERY + numRows));
    List<String> titles = new ArrayList<>();

    while (resultSet.next()) {
      titles.add(resultSet.getCurrentRowAsStruct().getString("title"));
    }

    return resultSet;
  }

  /**
   * querying with r2dbc driver.
   */
  @Benchmark
  public String testFirstResponseQueryingR2dbc(R2dbcConnectionState r2dbcState) {
    int numRows = 10;

    String result =
        Flux.from(r2dbcState.r2dbcConnection.createStatement(QUERY + numRows).execute())
            .flatMap(spannerResult -> spannerResult.map(
                (r, meta) -> r.get("title", String.class)
            )).blockFirst();

    return result;
  }

  /**
   * querying with client library.
   */
  @Benchmark
  public void tesFirstResponseQueryingClientLibrary(
      ClientLibraryConnectionState clientLibraryState, Blackhole blackhole) {

    int numRows = 10;

    ResultSet resultSet = clientLibraryState.dbClient.singleUse()
        .executeQuery(Statement.of(QUERY + numRows));
    resultSet.next();
    String result = resultSet.getCurrentRowAsStruct().getString("title");

    blackhole.consume(result);

    resultSet.close();
  }


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
