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
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class ClientLibraryBasedIntegrationTest {


  @Test
  public void testSessionCreation() {
    ConnectionFactory connectionFactory =
        ConnectionFactories.get(ConnectionFactoryOptions.builder()
            .option(Option.valueOf("project"), ServiceOptions.getDefaultProjectId())
            .option(DRIVER, DRIVER_NAME)
            .option(INSTANCE, DatabaseProperties.INSTANCE)
            .option(DATABASE, DatabaseProperties.DATABASE)
            .option(Option.valueOf("client-implementation"), "client-library")
            .build());

    Connection conn = Mono.from(connectionFactory.create())
        .block();

    assertThat(conn).isInstanceOf(SpannerClientLibraryConnection.class);
  }

  // TODO: refactor connection factory
  @Test
  public void testReadQuery() {

    Hooks.onOperatorDebug();
    ConnectionFactory connectionFactory =
        ConnectionFactories.get(ConnectionFactoryOptions.builder()
            .option(Option.valueOf("project"), ServiceOptions.getDefaultProjectId())
            .option(DRIVER, DRIVER_NAME)
            .option(INSTANCE, DatabaseProperties.INSTANCE)
            .option(DATABASE, DatabaseProperties.DATABASE)
            .option(Option.valueOf("client-implementation"), "client-library")
            .build());

    Connection conn = Mono.from(connectionFactory.create())
        .block();

    StepVerifier.create(
        Mono.from(conn.createStatement("SELECT 1").execute())
          .flatMapMany(rs -> rs.map((row, rmeta) -> (Long)row.get(1)))
    ).expectNext(Long.valueOf(1))
        .verifyComplete();
  }
}
