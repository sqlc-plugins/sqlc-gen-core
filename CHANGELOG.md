# Changelog

## 0.1.0 (2025-11-23)


### ⚠ BREAKING CHANGES

* Database struct replaced with CatalogBuilder, changes public API
* **schema:** Public API completely restructured - SchemaBuilder removed, all type names changed, and parse_sql method signature modified
* `parse` and `parse_from_settings` methods now take `&mut self` instead of `self` and return `Result<(), _>` instead of `Result<Self, _>`. This changes the API from a builder pattern to an in-place mutation pattern.

### Features

* add database constraints support and restructure schema parsing ([692b509](https://github.com/sqlc-plugins/sqlc-gen-core/commit/692b509ba0c62178970c4353bcf815ee9079f513))
* add runtime module for sqlc.dev plugin execution ([9d11ac6](https://github.com/sqlc-plugins/sqlc-gen-core/commit/9d11ac60d83f1ec9c7a532a6b86008a898ce37d5))
* add SQL schema parsing and constraint extraction ([9e70009](https://github.com/sqlc-plugins/sqlc-gen-core/commit/9e70009bd0448ff02490f8ca6e6fc2a6d5a6badc))
* initial Rust crate for building sqlc plugins ([8f35804](https://github.com/sqlc-plugins/sqlc-gen-core/commit/8f358040c376933727d023f626af4a54ab748df4))


### Code Refactoring

* change SchemaDef parse methods to use mutable references ([299093e](https://github.com/sqlc-plugins/sqlc-gen-core/commit/299093e484399750c6a97a0704c8a55274d456e8))
* **schema:** restructure API from builder pattern to database-centric design ([456835a](https://github.com/sqlc-plugins/sqlc-gen-core/commit/456835ad6cc9e7ab9a479cf0410ec3d6a79bbb56))
