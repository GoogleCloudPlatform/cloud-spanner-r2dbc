# Cloud Spanner Spring Data R2DBC

The Spring Data R2DBC Dialect for Cloud Spanner enables the usage of [Spring Data R2DBC](https://github.com/spring-projects/spring-data-r2dbc) with Cloud Spanner.

The goal of the Spring Data project is to create easy and consistent ways of using data access technologies from Spring Framework applications.

## Setup

To begin using Spring Data R2DBC with Cloud Spanner, add the following dependencies to your project:

```xml
<dependencies>
    <!-- The Spring Data R2DBC framework -->
    <dependency>
          <groupId>org.springframework.data</groupId>
          <artifactId>spring-data-r2dbc</artifactId>
          <version>${spring-data-r2dbc.version}</version>
    </dependency>

    <!-- R2DBC Driver for Cloud Spanner -->
    <dependency>
        <groupId>com.google.cloud</groupId>
        <artifactId>cloud-spanner-r2dbc</artifactId>
        <version>${cloud-spanner-r2dbc-version}</version>
    </dependency>

    <!-- Dialect to enable Spring Data R2DBC for Cloud Spanner -->
    <dependency>
        <groupId>com.google.cloud</groupId>
        <artifactId>cloud-spanner-spring-data-r2dbc</artifactId>
        <version>${cloud-spanner-r2dbc-version}</version>
    </dependency>
</dependencies>
```

## Overview

Spring Data R2DBC allows you to use the convenient features of Spring Data in a Reactive environment.
These features include:

* Spring configuration support using Java based `@Configuration` classes.
* Annotation based mapping metadata.
* Automatic implementation of Repository interfaces including support.
* Support for Reactive Transactions
* Schema and data initialization utilities.

See the [Spring Data R2DBC documentation](https://docs.spring.io/spring-data/r2dbc/docs/1.0.x/reference/html/#reference) for more information on how to use Spring Data R2DBC.

## Sample Application

We provide a [sample application](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/tree/master/cloud-spanner-r2dbc-samples/cloud-spanner-spring-data-r2dbc-sample)
which demonstrates using the Spring Data R2DBC with Cloud Spanner in [Spring WebFlux](https://docs.spring.io/spring/docs/current/spring-framework-reference/web-reactive.html).
