# Cloud Spanner R2DBC Driver

[![experimental](http://badges.github.io/stability-badges/dist/experimental.svg)](http://github.com/badges/stability-badges)

An implementation of the [R2DBC](https://r2dbc.io/) driver for [Cloud Spanner](https://cloud.google.com/spanner/) is being developed in this repository.

## Mapping of Data Types
Cloud Spanner R2DBC Driver supports the following types:


| Spanner Type | Java type         |
|--------------|-------------------|
|Bool          |java.lang.Bolean   |
|Bytes         |byte[]             |
|Date          |java.time.LocalDate|
|Float64       |java.lang.Double   |
|Int64         |java.lang.Long     |
|Int64         |java.lang.Integer  |
|String        |java.lang.String   |
|Timestamp     |java.sql.Timestamp |
|Array         |Array-Variant of the corresponding Java type (e.g. Long[] for ARRAY\<INT64\>)|

See [Cloud Spanner documentation](https://cloud.google.com/spanner/docs/data-types) to learn more about Spanner types.
