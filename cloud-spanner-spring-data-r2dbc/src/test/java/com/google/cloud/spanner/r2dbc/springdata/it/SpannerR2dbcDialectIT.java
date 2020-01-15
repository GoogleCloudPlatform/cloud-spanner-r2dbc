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

package com.google.cloud.spanner.r2dbc.springdata.it;

import static com.google.cloud.spanner.r2dbc.SpannerConnectionFactoryProvider.INSTANCE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;

import com.google.cloud.ServiceOptions;
import com.google.cloud.spanner.r2dbc.springdata.it.entities.President;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.r2dbc.core.DatabaseClient;
import reactor.test.StepVerifier;

public class SpannerR2dbcDialectIT {

  private static final Logger logger = LoggerFactory.getLogger(SpannerR2dbcDialectIT.class);

  private static final String DRIVER_NAME = "spanner";

  private static final String TEST_INSTANCE = "reactivetest";

  private static final String TEST_DATABASE = "testdb";

  private static final ConnectionFactory connectionFactory =
      ConnectionFactories.get(ConnectionFactoryOptions.builder()
          .option(Option.valueOf("project"), ServiceOptions.getDefaultProjectId())
          .option(DRIVER, DRIVER_NAME)
          .option(INSTANCE, TEST_INSTANCE)
          .option(DATABASE, TEST_DATABASE)
          .build());

  private DatabaseClient databaseClient;

  @BeforeEach
  public void initializeTestEnvironment() {
    this.databaseClient = DatabaseClient.create(connectionFactory);
    try {
      this.databaseClient.execute(
          "CREATE TABLE PRESIDENT ("
              + "  NAME STRING(256) NOT NULL,"
              + "  START_YEAR INT64 NOT NULL"
              + ") PRIMARY KEY (NAME)")
          .fetch()
          .rowsUpdated()
          .block();
    } catch (Exception e) {
      logger.error("Failed to create table; this is ok if it already existed: ", e);
    }
  }

  @AfterEach
  public void deleteTables() {
    this.databaseClient.execute("DROP TABLE PRESIDENT")
        .fetch()
        .rowsUpdated()
        .block();
  }

  @Test
  public void testReadWrite() {
    this.databaseClient.insert()
        .into(President.class)
        .using(new President("Bill Clinton", 1994))
        .then()
        .as(StepVerifier::create)
        .verifyComplete();

    this.databaseClient.select()
        .from(President.class)
        .fetch()
        .first()
        .as(StepVerifier::create)
        .expectNextMatches(
            president -> president.getName().equals("Bill Clinton")
                && president.getStartYear() == 1994)
        .verifyComplete();
  }
}
