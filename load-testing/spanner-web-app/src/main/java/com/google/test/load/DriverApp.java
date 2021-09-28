package com.google.test.load;

import com.google.cloud.ServiceOptions;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.SessionPoolOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class DriverApp {

  @Autowired
  public ServerProperties props;

  public static void main(String[] args) {
    SpringApplication.run(DriverApp.class, args);
  }

  @Bean
  public Spanner spanner() {
   SpannerOptions options = SpannerOptions.newBuilder()
        .setSessionPoolOption(
            SessionPoolOptions.newBuilder()
                .setMinSessions(500)
                .setMaxSessions(1000)
                .build())
       .setNumChannels(256)
       .build();

/*
    SpannerOptions options = SpannerOptions.newBuilder().build();
*/

    return options.getService();
  }

  @Bean
  public DatabaseClient spannerDatabaseClient(Spanner spanner) {
    System.out.println("**** Hi, server properties: " + props.getTomcat().getThreads().getMax());
    DatabaseId db =
        DatabaseId.of(ServiceOptions.getDefaultProjectId(), "loadtest", "store");
    return spanner.getDatabaseClient(db);
  }
}
