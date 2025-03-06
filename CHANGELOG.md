# Changelog

## [1.4.0](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/compare/v1.3.0...v1.4.0) (2025-03-06)


### Features

* implement getMetadata for SpannerClientLibraryConnection ([#867](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/issues/867)) ([06f4776](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/commit/06f4776caa87b46fa4a9813b612e3b9105a15d01))


### Bug Fixes

* add `sync-repo-settings.yaml` ([#822](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/issues/822)) ([4292fb0](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/commit/4292fb0697985a3eed2b6f4aeec428fd84495630))
* add java17 in unit tests ([#808](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/issues/808)) ([2d0bc6f](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/commit/2d0bc6fcc4bad7a62e0a320e356cdabcfaca42ee))
* command in README ([#864](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/issues/864)) ([270af62](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/commit/270af62b74d56662665acbf79b8a598bec936475))
* Deprecation notice: v3 of the artifact actions ([#869](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/issues/869)) ([346173a](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/commit/346173ad3273dcdc841f926cc16aee6eb655eb0e))
* Incorrect mapping for PROJECT, INSTANCE, DATABASE for log ([#863](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/issues/863)) ([fcde347](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/commit/fcde3479389be46d4b99dfdc863db33bfabaeb6b))
* upgrade action/cache ([#874](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/issues/874)) ([7bbd57e](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/commit/7bbd57ed35c50e8454ba7f3cbc67a16470ce6853))


### Documentation

* Add contribution guidelines ([#791](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/issues/791)) ([ee677b5](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/commit/ee677b56e4df1648129961445258b4ac6a744dbf))
* fix headers ([#796](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/issues/796)) ([4b98a1a](https://github.com/GoogleCloudPlatform/cloud-spanner-r2dbc/commit/4b98a1a4fb5c0121aec918941543c050bdf870ea))

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
