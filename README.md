# Cloud Spanner R2DBC Driver

[![experimental](http://badges.github.io/stability-badges/dist/experimental.svg)](http://github.com/badges/stability-badges)

An implementation of the [R2DBC](https://r2dbc.io/) driver for [Cloud Spanner](https://cloud.google.com/spanner/) is being developed in this repository.

## Setup Instructions

The sections below describe how to setup and begin using the Cloud Spanner R2DBC driver.

An overview of the steps are as follows:

1. Add the Cloud Spanner R2DBC driver dependency to your build configuration.
2. Configure the driver credentials/authentication for your Google Cloud Platform project to access
    Cloud Spanner.
3. Instantiate the R2DBC `ConnectionFactory` in Java code to build Connections and run queries.

### Maven Coordinates

```
<dependency>
  <groupId>com.google.cloud</groupId>
  <artifactId>cloud-spanner-r2dbc</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

### Gradle Coordinates

```
dependencies {
  compile group: 'com.google.cloud', name: 'cloud-spanner-r2dbc', version: '0.1.0-SNAPSHOT'
}
```

### Authentication

By default, the R2DBC driver will attempt to infer your account credentials from the environment
in which the application is run. There are a number of different ways to conveniently provide
account credentials to the driver.

#### Google Cloud SDK

Google Cloud SDK is a command line interface for Google Cloud Platform products and services.
This is the recommended way of setting up authentication during local development.

If you are using the SDK, the driver can automatically infer your account credentials from your
SDK configuration.

**Steps:**

1. Install the [Google Cloud SDK](https://cloud.google.com/sdk/) for command line and
    follow the [Cloud SDK quickstart](https://cloud.google.com/sdk/docs/quickstarts)
    for your operating system.
    
2. Once setup, run `gcloud auth application-default login` and login with your Google account
    credentials. 

After completing the SDK configuration, the Spanner R2DBC driver will automatically pick up your
credentials allowing you to access your Spanner database. 

#### Service Account

A [Google Service Account](https://cloud.google.com/iam/docs/understanding-service-accounts) is a
special type of Google Account intended to represent a non-human user that needs to authenticate
and be authorized to access your Google Cloud resources. Each service account has an account key
JSON file that is used as the credentials to accessing the resources of your account.
This is the recommended method of authentication for production use.

Setting up credentials for your service account can be done by following [these instructions](https://cloud.google.com/iam/docs/creating-managing-service-accounts).

#### Google Cloud Platform Environment

If your application is running on Google Cloud Platform infrastructure including: Compute Engine,
Kubernetes Engine, the App Engine flexible environment, or Cloud Functions, the credentials will
be automatically inferred from the runtime environment in the Cloud. For more information, see
the [Google Cloud Platform Authentication documentation](https://cloud.google.com/docs/authentication/production#obtaining_credentials_on_compute_engine_kubernetes_engine_app_engine_flexible_environment_and_cloud_functions).

### Usage

After setting up the dependency and authentication, one can begin directly using the driver.

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
| `GOOGLE_CREDENTIALS` | Optional [Google credentials](https://cloud.google.com/docs/authentication/production) override to specify for your Google Cloud account. | False | If not provided, credentials will be [inferred from your runtime environment](https://cloud.google.com/docs/authentication/production#finding_credentials_automatically).
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
