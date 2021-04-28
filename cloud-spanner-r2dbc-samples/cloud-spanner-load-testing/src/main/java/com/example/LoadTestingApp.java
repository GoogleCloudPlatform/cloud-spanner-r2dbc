package com.example;

import static com.example.DbConstants.TEST_DATABASE;
import static com.example.DbConstants.TEST_INSTANCE;
import static com.example.DbConstants.TEST_PROJECT;
import static com.google.cloud.spanner.r2dbc.SpannerConnectionFactoryProvider.DRIVER_NAME;
import static com.google.cloud.spanner.r2dbc.SpannerConnectionFactoryProvider.INSTANCE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;

import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class LoadTestingApp {

  public static void main(String[] args) {
    SpringApplication.run(LoadTestingApp.class, args);
  }

  @Bean
  public static ConnectionFactory spannerClientLibraryConnectionFactory() {
    ConnectionFactory connectionFactoryClientLibrary = ConnectionFactories.get(
        ConnectionFactoryOptions.builder()
            .option(Option.valueOf("project"), TEST_PROJECT)
            .option(DRIVER, DRIVER_NAME)
            .option(INSTANCE, TEST_INSTANCE)
            .option(DATABASE, TEST_DATABASE)
            .option(Option.valueOf("client-implementation"), "client-library")
            .build());

    return connectionFactoryClientLibrary;
  }
}
