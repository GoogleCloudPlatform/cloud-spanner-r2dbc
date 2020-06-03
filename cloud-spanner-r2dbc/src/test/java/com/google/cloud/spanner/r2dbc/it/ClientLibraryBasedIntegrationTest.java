package com.google.cloud.spanner.r2dbc.it;

import static com.google.cloud.spanner.r2dbc.SpannerConnectionFactoryProvider.DRIVER_NAME;
import static com.google.cloud.spanner.r2dbc.SpannerConnectionFactoryProvider.INSTANCE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.cloud.ServiceOptions;
import com.google.cloud.spanner.r2dbc.SpannerConnection;
import com.google.cloud.spanner.r2dbc.v2.SpannerClientLibraryConnection;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import java.time.LocalDate;
import java.time.Month;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class ClientLibraryBasedIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClientLibraryBasedIntegrationTest.class);

  private static final ConnectionFactory connectionFactory =
      ConnectionFactories.get(ConnectionFactoryOptions.builder()
          .option(Option.valueOf("project"), ServiceOptions.getDefaultProjectId())
          .option(DRIVER, DRIVER_NAME)
          .option(INSTANCE, DatabaseProperties.INSTANCE)
          .option(DATABASE, DatabaseProperties.DATABASE)
          .option(Option.valueOf("client-implementation"), "client-library")
          .build());


  // TODO: also clear table before each
  @BeforeAll
  public static void setupSpannerTable() throws InterruptedException, ExecutionException {

    Hooks.onOperatorDebug();

    SpannerClientLibraryConnection con =
        Mono.from(connectionFactory.create())
            .cast(SpannerClientLibraryConnection.class)
            .block();

    try {
      Mono.from(con.createStatement("DROP TABLE BOOKS").execute()).block();
    } catch (Exception e) {
      LOGGER.info("The BOOKS table doesn't exist", e);
    }

    Mono.from(con.createStatement(
        "CREATE TABLE BOOKS ("
            + "  UUID STRING(36) NOT NULL,"
            + "  TITLE STRING(256) NOT NULL,"
            + "  AUTHOR STRING(256) NOT NULL,"
            + "  SYNOPSIS STRING(MAX),"
            + "  EDITIONS ARRAY<STRING(MAX)>,"
            + "  FICTION BOOL NOT NULL,"
            + "  PUBLISHED DATE NOT NULL,"
            + "  WORDS_PER_SENTENCE FLOAT64 NOT NULL,"
            + "  CATEGORY INT64 NOT NULL"
            + ") PRIMARY KEY (UUID)").execute())
        .block();
  }

  @Test
  public void testSessionCreation() {

    Connection conn = Mono.from(connectionFactory.create())
        .block();

    assertThat(conn).isInstanceOf(SpannerClientLibraryConnection.class);
  }

  @Test
  public void testReadQuery() {

    Connection conn = Mono.from(connectionFactory.create())
        .block();

    StepVerifier.create(
        Mono.from(conn.createStatement("SELECT count(*) as count FROM BOOKS").execute())
          .flatMapMany(rs -> rs.map((row, rmeta) -> (Long)row.get(1)))
    ).expectNext(Long.valueOf(0))
        .verifyComplete();
	  StepVerifier.create(
			  Mono.from(conn.createStatement("SELECT count(*) as count FROM BOOKS").execute())
					  .flatMapMany(rs -> rs.map((row, rmeta) -> (Long)row.get("count")))
	  ).expectNext(Long.valueOf(0))
			  .verifyComplete();
  }

  @Test
  public void testDmlInsert() {
    Connection conn = Mono.from(connectionFactory.create())
        .block();

    StepVerifier.create(
        Mono.from(
            // TODO: replace hardcoded values with bind variables
            conn.createStatement(
                "INSERT BOOKS "
                    + "(UUID, TITLE, AUTHOR, CATEGORY, FICTION, "
                    + "PUBLISHED, WORDS_PER_SENTENCE)"
                    + " VALUES "
                    + "('abc123', 'White Fang', 'Jack London', 100, TRUE, '1906-05-01', 20.8);")
            .execute()
        ).flatMapMany(rs -> rs.getRowsUpdated())
    ).expectNext(1)
        .verifyComplete();

    StepVerifier.create(
        Mono.from(conn.createStatement("SELECT count(*) FROM BOOKS").execute())
            .flatMapMany(rs -> rs.map((row, rmeta) -> (Long)row.get(1)))
    ).expectNext(Long.valueOf(1))
        .verifyComplete();
  }
}
