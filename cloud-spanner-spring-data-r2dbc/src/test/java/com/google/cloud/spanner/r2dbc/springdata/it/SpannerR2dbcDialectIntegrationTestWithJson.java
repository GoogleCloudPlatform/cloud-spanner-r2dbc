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

package com.google.cloud.spanner.r2dbc.springdata.it;

import static com.google.cloud.spanner.r2dbc.SpannerConnectionFactoryProvider.INSTANCE;
import static com.google.cloud.spanner.r2dbc.SpannerConnectionFactoryProvider.PROJECT;
import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;

import com.google.cloud.ServiceOptions;
import com.google.cloud.spanner.r2dbc.springdata.it.entities.Person;
import com.google.cloud.spanner.r2dbc.v2.JsonWrapper;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration;
import org.springframework.data.r2dbc.convert.R2dbcCustomConversions;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Integration tests for the Spring Data R2DBC dialect Json support.
 *
 * <p>By default, the test is configured to run tests in the `reactivetest` instance on the `testdb`
 * database. This can be configured by overriding the `spanner.instance` and `spanner.database`
 * system properties.
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = SpannerR2dbcDialectIntegrationTestWithJson.TestConfiguration.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SpannerR2dbcDialectIntegrationTestWithJson {

  private static final String PROJECT_NAME =
      System.getProperty("gcp.project", ServiceOptions.getDefaultProjectId());
  private static final String DRIVER_NAME = "spanner";

  private static final String TEST_INSTANCE =
      System.getProperty("spanner.instance", "reactivetest");

  private static final String TEST_DATABASE = System.getProperty("spanner.database", "testdb");

  private static final ConnectionFactory connectionFactory =
      ConnectionFactories.get(
          ConnectionFactoryOptions.builder()
              .option(Option.valueOf("project"), ServiceOptions.getDefaultProjectId())
              .option(PROJECT, PROJECT_NAME)
              .option(DRIVER, DRIVER_NAME)
              .option(INSTANCE, TEST_INSTANCE)
              .option(DATABASE, TEST_DATABASE)
              .build());

  private DatabaseClient databaseClient;

  @Autowired private R2dbcEntityTemplate r2dbcEntityTemplate;

  /** Initializes the integration test environment for the Spanner R2DBC dialect. */
  @BeforeAll
  public void initializeTestEnvironment() {
    Connection connection = Mono.from(connectionFactory.create()).block();

    this.databaseClient = this.r2dbcEntityTemplate.getDatabaseClient();

    if (SpannerTestUtils.tableExists(connection, "PERSON")) {
      this.databaseClient.sql("DROP TABLE PERSON").fetch().rowsUpdated().block();
    }

    this.databaseClient
        .sql(
            "CREATE TABLE PERSON ("
                + "  NAME STRING(256) NOT NULL,"
                + "  BIRTH_YEAR INT64 NOT NULL,"
                + "  EXTRAS JSON"
                + ") PRIMARY KEY (NAME)")
        .fetch()
        .rowsUpdated()
        .block();
  }

  @AfterEach
  public void cleanupTableAfterTest() {
    this.databaseClient
        .sql("DELETE FROM PERSON where NAME is not null")
        .fetch()
        .rowsUpdated()
        .block();
  }

  private void insertPerson(Person person) {
    this.r2dbcEntityTemplate
        .insert(Person.class)
        .using(person)
        .then()
        .as(StepVerifier::create)
        .verifyComplete();
  }

  @Test
  void testReadWriteWithJsonField() {
    Map<String, String> extras = new HashMap<>();
    extras.put("bio", "former U.S. president");
    extras.put("spouse", "Hillary Clinton");
    Person billClinton = new Person("Bill Clinton", 1946, extras);
    insertPerson(billClinton);

    this.r2dbcEntityTemplate
        .select(Person.class)
        .first()
        .as(StepVerifier::create)
        .expectNextMatches(
            person ->
                person.getName().equals("Bill Clinton")
                    && person.getBirthYear() == 1946
                    && person.getExtras().getOrDefault("spouse", "none").equals("Hillary Clinton")
                    && person
                        .getExtras()
                        .getOrDefault("bio", "none")
                        .equals("former U.S. president"))
        .verifyComplete();
  }

  /** Register custom converters between Map and JsonWrapper. */
  @Configuration
  static class TestConfiguration extends AbstractR2dbcConfiguration {

    @Autowired ApplicationContext applicationContext;

    @Override
    public ConnectionFactory connectionFactory() {
      return connectionFactory;
    }

    @Bean
    public Gson gson() {
      return new Gson();
    }

    @Bean
    @Override
    public R2dbcCustomConversions r2dbcCustomConversions() {
      List<Converter<?, ?>> converters = new ArrayList<>();
      converters.add(this.applicationContext.getBean(JsonToMapConverter.class));
      converters.add(this.applicationContext.getBean(MapToJsonConverter.class));
      return new R2dbcCustomConversions(getStoreConversions(), converters);
    }

    @Component
    @ReadingConverter
    public class JsonToMapConverter implements Converter<JsonWrapper, Map<String, String>> {

      private final Gson gson;

      @Autowired
      public JsonToMapConverter(Gson gson) {
        this.gson = gson;
      }

      @Override
      public Map<String, String> convert(JsonWrapper json) {
        try {
          return this.gson.fromJson(json.toString(), Map.class);
        } catch (JsonParseException e) {
          return new HashMap<>();
        }
      }
    }

    @Component
    @WritingConverter
    public class MapToJsonConverter implements Converter<Map<String, String>, JsonWrapper> {

      private final Gson gson;

      @Autowired
      public MapToJsonConverter(Gson gson) {
        this.gson = gson;
      }

      @Override
      public JsonWrapper convert(Map<String, String> source) {
        try {
          return JsonWrapper.of(this.gson.toJson(source));
        } catch (JsonParseException e) {
          return JsonWrapper.of("");
        }
      }
    }
  }
}
