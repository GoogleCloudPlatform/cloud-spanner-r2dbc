# Cloud Spanner R2DBC Driver

[![experimental](http://badges.github.io/stability-badges/dist/experimental.svg)](http://github.com/badges/stability-badges)

An implementation of the [R2DBC](https://r2dbc.io/) driver for [Cloud Spanner](https://cloud.google.com/spanner/) is being developed in this repository.

## Mapping of Data Types
Cloud Spanner R2DBC Driver supports the following types:


| Spanner Type | Java type         |
|--------------|-------------------|
|`BOOL`          |`java.lang.Bolean`   |
|`BYTES`         |`byte[]`             |
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