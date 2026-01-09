# DeltaMergeBuilder S7 Class

An S7 class representing a Delta Lake merge operation builder. All
configuration is stored in R and only passed to Rust on execute().

## Usage

``` r
DeltaMergeBuilder(
  table_path = character(0),
  storage_options = list(),
  source_data = NULL,
  predicate = character(0),
  source_alias = "source",
  target_alias = "target",
  matched_update_clauses = list(),
  matched_delete_clauses = list(),
  not_matched_insert_clauses = list(),
  not_matched_by_source_update_clauses = list(),
  not_matched_by_source_delete_clauses = list()
)
```

## Arguments

- table_path:

  Character. Path to the Delta table.

- storage_options:

  Named list or NULL. Storage backend options.

- source_data:

  The source data for the merge operation.

- predicate:

  Character. SQL-like predicate for matching rows.

- source_alias:

  Character. Alias for source table in expressions.

- target_alias:

  Character. Alias for target table in expressions.

- matched_update_clauses:

  List. WHEN MATCHED UPDATE clauses.

- matched_delete_clauses:

  List. WHEN MATCHED DELETE clauses.

- not_matched_insert_clauses:

  List. WHEN NOT MATCHED INSERT clauses.

- not_matched_by_source_update_clauses:

  List. WHEN NOT MATCHED BY SOURCE UPDATE clauses.

- not_matched_by_source_delete_clauses:

  List. WHEN NOT MATCHED BY SOURCE DELETE clauses.

## Details

This class is typically created by calling
[`delta_merge`](https://ixpantia.github.io/deltaR/reference/delta_merge.md)
rather than constructing it directly.

## See also

[`delta_merge`](https://ixpantia.github.io/deltaR/reference/delta_merge.md)
for creating merge operations.
