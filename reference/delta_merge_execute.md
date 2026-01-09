# Execute a Delta Lake MERGE operation

This function receives all merge configuration from R and executes the
merge in a single call, avoiding complex state management.

## Usage

``` r
delta_merge_execute(
  table_uri,
  source_stream,
  predicate,
  source_alias,
  target_alias,
  matched_update_clauses,
  matched_delete_clauses,
  not_matched_insert_clauses,
  not_matched_by_source_update_clauses,
  not_matched_by_source_delete_clauses,
  storage_options
)
```

## Arguments

- table_uri:

  Path to the Delta table

- source_stream:

  Arrow data stream for source data

- predicate:

  Main merge predicate (e.g., "target.id = source.id")

- source_alias:

  Alias for source table in expressions

- target_alias:

  Alias for target table in expressions

- matched_update_clauses:

  List of update clauses for matched rows

- matched_delete_clauses:

  List of delete clauses for matched rows

- not_matched_insert_clauses:

  List of insert clauses for unmatched source rows

- not_matched_by_source_update_clauses:

  List of update clauses for unmatched target rows

- not_matched_by_source_delete_clauses:

  List of delete clauses for unmatched target rows

- storage_options:

  Storage backend options (optional)
