# Changelog

## [1.3.0](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/compare/v1.2.2...v1.3.0) (2023-10-30)


### Features

* Implementing Connection#beginTransaction(TransactionDefinition) to support @Transactional annotation ([#738](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/issues/738)) ([bee69c1](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/commit/bee69c1e5a8c57969bc004d54bb9df79a441a16e))
* Native image support for R2DBC driver ([#715](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/issues/715)) ([44463cf](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/commit/44463cf11da5c4cee261e3dd6c49d9a124eb5d43))
* R2DBC SPI upgrade to 1.0.0.RELEASE ([#726](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/issues/726)) ([5ee8788](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/commit/5ee8788f56994996231a039d9bee4c2f2c1753f8))

## [1.2.2](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/compare/v1.2.1...v1.2.2) (2023-06-02)


### Bug Fixes

* Context loss after applying com.google.cloud.spanner.r2dbc.v2.SpannerClientLibraryStatement ([#690](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/issues/690)) ([93a34e0](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/commit/93a34e0e3450196c509df18cd6f182e97548bfd4))

## [1.2.1](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/compare/v1.2.0...v1.2.1) (2023-05-12)


### Bug Fixes

* row column indexing in SpannerClientLibraryRow ([#615](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/issues/615)) ([faca504](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/commit/faca50457615359da2766641aa2cccb916558bd9)), closes [#609](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/issues/609)
