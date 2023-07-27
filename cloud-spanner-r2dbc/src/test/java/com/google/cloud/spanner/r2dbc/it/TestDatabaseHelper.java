/*
 * Copyright 2021-2021 Google LLC
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

package com.google.cloud.spanner.r2dbc.it;

import static com.google.cloud.spanner.r2dbc.SpannerConnectionFactoryProvider.DRIVER_NAME;
import static com.google.cloud.spanner.r2dbc.SpannerConnectionFactoryProvider.INSTANCE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;

import com.google.cloud.ServiceOptions;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import java.util.Random;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Blocking (non-reactive) helper for set up/tear down of integration test data.
 */
class TestDatabaseHelper {

  private static final String SUFFIX = getTableSuffix();

  public static final String BOOKS_TABLE = "BOOKS" + SUFFIX;
  private static final Logger LOGGER = LoggerFactory.getLogger(TestDatabaseHelper.class);

  private static final String INSERT_DATA_QUERY = String.format(
      "INSERT %s (UUID, TITLE, CATEGORY)  VALUES (@uuid, @title, @category)", BOOKS_TABLE);

  private final Connection connection;

  Random random = new Random();

  public TestDatabaseHelper(ConnectionFactory connectionFactory) {

    this.connection = Mono.from(connectionFactory.create()).block();
  }

  public void dropTable() {
    LOGGER.info(String.format("Dropping table %s.", BOOKS_TABLE));

    try {
      Mono.from(this.connection.createStatement(String.format("DROP TABLE %s", BOOKS_TABLE))
          .execute()).block();
    } catch (Exception e) {
      LOGGER.info(String.format("The %s table doesn't exist", BOOKS_TABLE), e);
    }
  }

  public void createTableIfNecessary() {
    boolean exists = tableExists(BOOKS_TABLE);
    if (exists && "false".equals(System.getProperty("it.recreate-ddl"))) {
      return;
    }

    if (exists) {
      dropTable();
    }

    createTable();
  }

  public boolean tableExists(String tableName) {
    return Flux.from(
        this.connection
          .createStatement(
              "SELECT table_name FROM information_schema.tables WHERE table_name = @name")
          .bind("name", tableName)
          .execute()
    )
        .flatMap(result -> result.map((r, m) -> r))
        .log()
        .hasElements()
        .block();
  }

  private void createTable() {
    LOGGER.info(String.format("Creating table %s.", BOOKS_TABLE));
    Mono.from(
        this.connection.createStatement(
                "CREATE TABLE " + BOOKS_TABLE + " ("
                    + "  UUID STRING(36) NOT NULL,"
                    + "  TITLE STRING(256) NOT NULL,"
                    + "  AUTHOR STRING(256),"
                    + "  SYNOPSIS STRING(MAX),"
                    + "  EDITIONS ARRAY<STRING(MAX)>,"
                    + "  AWARDS ARRAY<STRING(MAX)>,"
                    + "  FICTION BOOL,"
                    + "  PUBLISHED DATE,"
                    + "  WORDS_PER_SENTENCE FLOAT64,"
                    + "  CATEGORY INT64,"
                    + "  PRICE NUMERIC,"
                    + "  EXTRA JSON"
                    + ") PRIMARY KEY (UUID)")
            .execute())
        .block();
  }

  public void addTestData(int numRows) {

    for (int i = 0; i < numRows; i++) {
      int suffix = Math.abs(this.random.nextInt());
      Mono.from(
              this.connection
                  .createStatement(INSERT_DATA_QUERY)
                  .bind("uuid", "autoinserted-" + suffix)
                  .bind("title", "title" + suffix)
                  .bind("category", suffix)
                  .execute())
          .flatMapMany(rs -> rs.getRowsUpdated())
          .blockLast();
    }
  }

  public void addTestData(String title, int category) {
    int suffix = Math.abs(this.random.nextInt());
    Mono.from(
        this.connection
            .createStatement(INSERT_DATA_QUERY)
            .bind("uuid", "autoinserted-" + suffix)
            .bind("title", title)
            .bind("category", category)
            .execute())
        .flatMapMany(rs -> rs.getRowsUpdated())
        .blockLast();
  }

  public void clearTestData() {

    Mono.from(
        this.connection.createStatement(String.format("DELETE FROM %s WHERE true", BOOKS_TABLE))
            .execute())
        .flatMap(rs -> Mono.from(rs.getRowsUpdated()))
        .block();
  }

  public void close() {
    Mono.from(this.connection.close()).block();
  }

  public void dropTableIfUsingWithRandomSuffix() {
    boolean useRandomSuffix = Boolean.parseBoolean(
        System.getProperty("it.use-random-suffix", "false"));
    if (useRandomSuffix) {
      dropTable();
    }
  }

  private static String getTableSuffix() {
    boolean useRandomSuffix = Boolean.parseBoolean(
        System.getProperty("it.use-random-suffix", "false"));
    if (useRandomSuffix) {
      return "_" + UUID.randomUUID().toString().split("-")[0];
    }
    return "";
  }

  public static void main(String[] args) {
    ConnectionFactory cf =
        ConnectionFactories.get(
            ConnectionFactoryOptions.builder()
                .option(Option.valueOf("project"), ServiceOptions.getDefaultProjectId())
                .option(DRIVER, DRIVER_NAME)
                .option(INSTANCE, DatabaseProperties.INSTANCE)
                .option(DATABASE, DatabaseProperties.DATABASE)
                .build());

    TestDatabaseHelper helper = new TestDatabaseHelper(cf);
    helper.clearTestData();
    helper.addTestData(5);
    helper.close();
  }
}
