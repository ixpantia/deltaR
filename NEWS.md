# deltaR (development version)

## New Features

* **MERGE operations**: New `delta_merge()` function enables sophisticated data manipulation:
  - `when_matched_update()` and `when_matched_update_all()` for updating matched rows
  - `when_matched_delete()` for deleting matched rows
  - `when_not_matched_insert()` and `when_not_matched_insert_all()` for inserting new rows
  - `when_not_matched_by_source_update()` and `when_not_matched_by_source_delete()` for handling rows only in target
  - Full support for conditional predicates on all clauses
  - Returns detailed metrics (rows inserted, updated, deleted, etc.)

# deltaR 0.1.0

## New Features

* Initial release of deltaR, an R interface to Delta Lake.
* **Reading Delta tables**: Open and query Delta tables with `delta_table()`.
* **Writing Delta tables**: Create and modify Delta tables with `write_deltalake()`.
* **Time travel**: Access historical versions with `load_version()` and `load_datetime()`.
* **Schema evolution**: Support for `schema_mode = "merge"` and `schema_mode = "overwrite"`.
* **Partitioned tables**: Create partitioned tables with the `partition_by` parameter.
* **Cloud storage support**: Native support for Amazon S3, Google Cloud Storage, and Azure Blob Storage.
* **Table maintenance**: `vacuum()` method to remove old files.
* **Table creation**: `create_deltalake()` to create empty tables with a predefined schema.

## Acknowledgments
This package is built on the excellent [delta-rs](https://github.com/delta-io/delta-rs) Rust library. We are grateful to the delta-rs maintainers and the broader Delta Lake community for their work.