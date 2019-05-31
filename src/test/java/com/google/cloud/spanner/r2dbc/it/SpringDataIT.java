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

package com.google.cloud.spanner.r2dbc.it;

import static com.google.cloud.spanner.r2dbc.SpannerConnectionFactoryProvider.INSTANCE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.cloud.ServiceOptions;
import com.google.cloud.spanner.r2dbc.SpannerConnectionFactory;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.r2dbc.connectionfactory.R2dbcTransactionManager;
import org.springframework.data.r2dbc.core.DatabaseClient;
import org.springframework.data.r2dbc.core.TransactionalDatabaseClient;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

public class SpringDataIT {

  private static final String DRIVER_NAME = "spanner";

  private static final String TEST_INSTANCE = "reactivetest";

  private static final String TEST_DATABASE = "testdb";

  private static final ConnectionFactory connectionFactory =
      ConnectionFactories.get(ConnectionFactoryOptions.builder()
          // TODO: consider whether to bring autodiscovery of project ID
          .option(Option.valueOf("project"), ServiceOptions.getDefaultProjectId())
          .option(DRIVER, DRIVER_NAME)
          .option(INSTANCE, TEST_INSTANCE)
          .option(DATABASE, TEST_DATABASE)
          .build());

  private DatabaseClient databaseClient;

  @Before
  public void setupDatabaseClient() {
    this.databaseClient = DatabaseClient.create(connectionFactory);
  }

  @Test
  public void tryDmlQuery() {
    // NOTES:
    // Usage of databaseClient without the TransactionalOperator does not support DML statements
    // it will execute in auto-commit mode; we assume the user will use read-only transactions in
    // auto-commit mode.
    ReactiveTransactionManager tm = new R2dbcTransactionManager(connectionFactory);
    TransactionalOperator operator = TransactionalOperator.create(tm);

    int rowsUpdated = databaseClient
        .insert()
        .into("POKEMONS")
        .value("name", "pikachu")
        .value("level", 99)
        .fetch()
        .rowsUpdated()
        .as(operator::transactional)
        .block();

    assertThat(rowsUpdated).isEqualTo(1);
  }

  @Test
  public void tryReadQuery() {
    List<String> rows = databaseClient
        .select()
        .from("POKEMONS")
        .project("name", "level")
        .orderBy(Sort.by(Order.desc("name")))
        .map((row, rowMetadata) -> row.get(0, String.class))
        .all()
        .collectList()
        .block();

    System.out.println(rows);
    assertThat(rows).containsExactlyInAnyOrder("Pokachu", "Scyther");
  }
}
