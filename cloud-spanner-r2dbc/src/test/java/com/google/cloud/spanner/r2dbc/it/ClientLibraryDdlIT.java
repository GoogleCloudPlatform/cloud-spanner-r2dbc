package com.google.cloud.spanner.r2dbc.it;

import static com.google.cloud.spanner.r2dbc.SpannerConnectionFactoryProvider.DRIVER_NAME;
import static com.google.cloud.spanner.r2dbc.SpannerConnectionFactoryProvider.INSTANCE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;

import com.google.cloud.ServiceOptions;
import io.r2dbc.spi.Closeable;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import io.r2dbc.spi.Result;
import java.util.Random;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class ClientLibraryDdlIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClientLibraryDdlIT.class);

  private static final ConnectionFactory connectionFactory =
      ConnectionFactories.get(
          ConnectionFactoryOptions.builder()
              .option(Option.valueOf("project"), ServiceOptions.getDefaultProjectId())
              .option(DRIVER, DRIVER_NAME)
              .option(INSTANCE, DatabaseProperties.INSTANCE)
              .option(DATABASE, DatabaseProperties.DATABASE)
              .build());

  Random random = new Random();

  @AfterAll
  static void cleanUpEnvironment() {
    Closeable closeableConnectionFactory = (Closeable) connectionFactory;
    Mono.from(closeableConnectionFactory.close()).block();
  }

  @Test
  void ddlCreateAndDrop() {
    Hooks.onOperatorDebug();
    String listTables = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name=@table";
    String tableName = "test_table_" + this.random.nextInt(100000);

    Connection conn = Mono.from(connectionFactory.create()).block();

    StepVerifier.create(
        Flux.from(conn.createStatement(listTables).bind("table", tableName).execute())
            .flatMap(this::getFirstNumber)
    ).expectNext(0L).as("Table not found before creation").verifyComplete();

    StepVerifier.create(
        Mono.from(conn.createStatement(
            "CREATE TABLE " + tableName + " ("
                + "  NAME STRING(256) NOT NULL,"
                + "  START_YEAR INT64 NOT NULL"
                + ") PRIMARY KEY (NAME)")
            .execute())
            .log("Table" + tableName + " created")
            .flatMap(res -> Mono.from(res.getRowsUpdated()))
    ).expectNext(0).as("DDL execution returns zero affected rows")
        .verifyComplete();

    StepVerifier.create(
        Flux.from(conn.createStatement(listTables).bind("table", tableName).execute())
            .flatMap(this::getFirstNumber)
    ).expectNext(1L).as("Table found after creation").verifyComplete();

    StepVerifier.create(
        Flux.from(conn.createStatement("DROP TABLE " + tableName).execute())
            .log("Table " + tableName + " dropped")
            .flatMap(res -> res.map(
                (row, meta) -> "this should not happen because DDL does not return rows")))
        .expectNext(/* nothing */).as("DDL execution returns zero affected rows")
        .verifyComplete();

    StepVerifier.create(
        Flux.from(conn.createStatement(listTables).bind("table", tableName).execute())
            .flatMap(this::getFirstNumber)
    ).expectNext(0L).as("Table not found after deletion").verifyComplete();
  }

  private Publisher<Long> getFirstNumber(Result result) {
    return result.map((row, meta) -> (Long) row.get(1));
  }
}
