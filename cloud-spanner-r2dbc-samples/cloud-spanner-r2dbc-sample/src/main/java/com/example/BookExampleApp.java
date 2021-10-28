/*
 * Copyright 2019-2020 Google LLC
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

package com.example;

import static com.google.cloud.spanner.r2dbc.SpannerConnectionFactoryProvider.DRIVER_NAME;
import static com.google.cloud.spanner.r2dbc.SpannerConnectionFactoryProvider.INSTANCE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;

import com.google.cloud.spanner.r2dbc.v2.JsonHolder;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * The main class for the functions of the sample application.
 */
public class BookExampleApp {

  private final ConnectionFactory connectionFactory;

  private final Connection connection;

  /**
   * Constructor.
   *
   * @param sampleInstance the sample instance to use.
   * @param sampleDatabase the sample database to use.
   * @param sampleProjectId the sample project to use.
   */
  public BookExampleApp(String sampleInstance, String sampleDatabase,
      String sampleProjectId) {
    this.connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
        .option(Option.valueOf("project"), sampleProjectId)
        .option(DRIVER, DRIVER_NAME)
        .option(INSTANCE, sampleInstance)
        .option(DATABASE, sampleDatabase)
        .build());

    this.connection = Mono.from(this.connectionFactory.create()).block();
  }

  /**
   * Cleans up records created in sample.
   */
  public void cleanup() {

    Mono.from(this.connection.createStatement("DELETE FROM BOOKS where id is not null").execute())
            .doOnSuccess(x -> System.out.println("Table rows deletion completed."))
            .block();
    Mono.from(this.connection.close()).block();
  }

  /**
   * Creates a table named BOOKS.
   */
  public void createTable() {
    Mono.from(this.connection.createStatement("CREATE TABLE BOOKS ("
            + "  ID STRING(20) NOT NULL,"
            + "  TITLE STRING(MAX) NOT NULL,"
            + "  PRICE INT64,"
            + "  JSONFIELD JSON"
            + ") PRIMARY KEY (ID)").execute())
        .doOnSuccess(x -> System.out.println("Table creation completed."))
        .block();
  }

  /**
   * Saves two books.
   */
  public void saveBooks() {
    Statement statement = this.connection.createStatement(
        "INSERT BOOKS "
            + "(ID, TITLE, PRICE)"
            + " VALUES "
            + "(@id, @title, @price)")
        .bind("id", "book1")
        .bind("title", "Book One")
        .bind("price", 33)
        .add()
        .bind("id", "book2")
        .bind("title", "Book Two")
        .bind("price", 45)
        .add();

    Flux.concat(this.connection.beginTransaction(),
        Flux.from(statement.execute()).flatMapSequential(r -> Mono.from(r.getRowsUpdated())).then(),
        this.connection.commitTransaction()
    ).doOnComplete(() -> System.out.println("Insert books transaction committed."))
        .blockLast();


    Statement statement2 = this.connection.createStatement(
            "INSERT BOOKS "
                    + "(ID, TITLE, PRICE, JSONFIELD)"
                    + " VALUES "
                    + "(@id, @title, @price, @jsonfield)")
            .bind("id", "book3")
            .bind("title", "Book Three")
            .bind("price", 133)
            .bind("jsonfield", new JsonHolder("{\"rating\":9,\"series\":true}"))
            .add();

    Flux.concat(
            this.connection.beginTransaction(),
            Flux.from(statement2.execute())
                .flatMapSequential(r -> Mono.from(r.getRowsUpdated()))
                .then(),
            this.connection.commitTransaction())
        .doOnComplete(
            () -> System.out.println("Insert 3rd book with JSON field transaction committed."))
        .blockLast();
  }

  /**
   * Finds books in the table named BOOKS.
   */
  public void retrieveBooks() {
    Flux.from(this.connection.createStatement("SELECT * FROM books").execute())
        .flatMap(
            spannerResult ->
                spannerResult.map(
                    (r, meta) -> {
                      if (r.get("JSONFIELD", JsonHolder.class) != null) {
                        return "Retrieved book: "
                            + r.get("ID", String.class)
                            + "; Title: "
                            + r.get("TITLE", String.class)
                            + "; Extra Details: "
                            + r.get("JSONFIELD", JsonHolder.class).getJsonVal().getJson();
                      }
                      return "Retrieved book: "
                          + r.get("ID", String.class)
                          + "; Title: "
                          + r.get("TITLE", String.class);
                    }))
        .doOnNext(System.out::println)
        .collectList()
        .block();
  }

  /**
   * Drops the BOOKS table.
   */
  public void dropTableIfPresent() {
    try {
      Mono.from(this.connection.createStatement("DROP TABLE BOOKS").execute())
          .doOnNext(x -> System.out.println("Table drop completed."))
          .block();
    } catch (Exception e) {
      System.out.println("Table wasn't found, so no action was taken.");
    }
  }
}
