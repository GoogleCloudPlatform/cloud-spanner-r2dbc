# Cloud Spanner R2DBC Driver

[![experimental](http://badges.github.io/stability-badges/dist/experimental.svg)](http://github.com/badges/stability-badges)

An implementation of the [R2DBC](https://r2dbc.io/) driver for [Cloud Spanner](https://cloud.google.com/spanner/) is being developed in this repository.

## Setup Instructions

This section describes how to setup and begin using the Cloud Spanner R2DBC driver.

### Maven

Below is the Maven coordinates for this driver:

```
<dependency>
  <groupId>com.google.cloud</groupId>
  <artifactId>cloud-spanner-r2dbc</artifactId>
  <version> TBD </version>
</dependency>
```

### Usage

The entry point to using the R2DBC driver is to first configure the R2DBC connection factory.

```
import static com.google.cloud.spanner.r2dbc.SpannerConnectionFactoryProvider.PROJECT;
import static com.google.cloud.spanner.r2dbc.SpannerConnectionFactoryProvider.INSTANCE;

ConnectionFactory connectionFactory =
    ConnectionFactories.get(ConnectionFactoryOptions.builder()
        .option(DRIVER, "spanner")
        .option(PROJECT, "your-gcp-project-id")
        .option(INSTANCE, "your-spanner-instance")
        .option(DATABASE, "your-database-name")
        .build());
        
// The R2DBC connection may now be created.
Publisher<? extends Connection> connectionPublisher = connectionFactory.create();
```

The following options are available to be configured for the connection factory:

| Option Name | Description                | Required | Default Value |
|-------------|----------------------------|----------|---------------|
| `DRIVER`    | Must be "spanner"          | True     |               |
| `PROJECT`   | Your GCP Project ID        | True     |               |
| `INSTANCE`  | Your Spanner Instance name | True     |               |
| `DATABASE`  | Your Spanner Database name | True     |               |
| `GOOGLE_CREDENTIALS` | Optional [Google credentials](https://cloud.google.com/docs/authentication/production) to specify for your Google Cloud account. | False | If not provided, credentials will be [inferred from your runtime environment](https://cloud.google.com/docs/authentication/production#finding_credentials_automatically).
| `PARTIAL_RESULT_SET_FETCH_SIZE` | Number of intermediate result sets that are buffered in transit for a read query. | False | 1 |

## Mapping of Data Types

Cloud Spanner R2DBC Driver supports the following types:


| Spanner Type   | Java type           |
|----------------|---------------------|
|`BOOL`          |`java.lang.Bolean`   |
|`BYTES`         |`java.nio.ByteBuffer`|
|`DATE`          |`java.time.LocalDate`|
|`FLOAT64`       |`java.lang.Double`   |
|`INT64`         |`java.lang.Long`     |
|`INT64`         |`java.lang.Integer`  |
|`STRING`        |`java.lang.String`   |
|`TIMESTAMP`     |`java.sql.Timestamp` |
|`ARRAY`         |Array-Variant of the corresponding Java type (e.g. `Long[]` for `ARRAY<INT64>`)|

Null values mapping is supported in both directions.

See [Cloud Spanner documentation](https://cloud.google.com/spanner/docs/data-types) to learn more about Spanner types.

## Back Pressure

Table rows are transmitted from Cloud Spanner in fragments called `PartialResultset`.
The number of fragments for each row cannot be determined beforehand. 
While you can decide the number of rows you request from `SpannerResult`, the Cloud Spanner R2DBC driver will always request a fixed number of fragments from Cloud Spanner to fulfill your request and will do so repeatedly if necessary.

The default number of fragments per request to Cloud Spanner is 1, but this can be configured with the `partial_result_set_fetch_size` config property for your situation.
