# Changelog

## deltaR (development version)

### New Features

- **MERGE operations**: New
  [`delta_merge()`](https://ixpantia.github.io/deltaR/reference/delta_merge.md)
  function enables sophisticated data manipulation:
  - [`when_matched_update()`](https://ixpantia.github.io/deltaR/reference/when_matched_update.md)
    and
    [`when_matched_update_all()`](https://ixpantia.github.io/deltaR/reference/when_matched_update_all.md)
    for updating matched rows
  - [`when_matched_delete()`](https://ixpantia.github.io/deltaR/reference/when_matched_delete.md)
    for deleting matched rows
  - [`when_not_matched_insert()`](https://ixpantia.github.io/deltaR/reference/when_not_matched_insert.md)
    and
    [`when_not_matched_insert_all()`](https://ixpantia.github.io/deltaR/reference/when_not_matched_insert_all.md)
    for inserting new rows
  - [`when_not_matched_by_source_update()`](https://ixpantia.github.io/deltaR/reference/when_not_matched_by_source_update.md)
    and
    [`when_not_matched_by_source_delete()`](https://ixpantia.github.io/deltaR/reference/when_not_matched_by_source_delete.md)
    for handling rows only in target
  - Full support for conditional predicates on all clauses
  - Returns detailed metrics (rows inserted, updated, deleted, etc.)

## deltaR 0.1.0

### New Features

- Initial release of deltaR, an R interface to Delta Lake.
- **Reading Delta tables**: Open and query Delta tables with
  [`delta_table()`](https://ixpantia.github.io/deltaR/reference/delta_table.md).
- **Writing Delta tables**: Create and modify Delta tables with
  [`write_deltalake()`](https://ixpantia.github.io/deltaR/reference/write_deltalake.md).
- **Time travel**: Access historical versions with
  [`load_version()`](https://ixpantia.github.io/deltaR/reference/load_version.md)
  and
  [`load_datetime()`](https://ixpantia.github.io/deltaR/reference/load_datetime.md).
- **Schema evolution**: Support for `schema_mode = "merge"` and
  `schema_mode = "overwrite"`.
- **Partitioned tables**: Create partitioned tables with the
  `partition_by` parameter.
- **Cloud storage support**: Native support for Amazon S3, Google Cloud
  Storage, and Azure Blob Storage.
- **Table maintenance**:
  [`vacuum()`](https://ixpantia.github.io/deltaR/reference/vacuum.md)
  method to remove old files.
- **Table creation**:
  [`create_deltalake()`](https://ixpantia.github.io/deltaR/reference/create_deltalake.md)
  to create empty tables with a predefined schema.

### Acknowledgments

This package is built on the excellent
[delta-rs](https://github.com/delta-io/delta-rs) Rust library. We are
grateful to the delta-rs maintainers and the broader Delta Lake
community for their work.
