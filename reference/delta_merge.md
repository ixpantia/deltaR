# Start a Delta Lake MERGE operation

Creates a DeltaMergeBuilder to configure and execute a MERGE operation
that can update, insert, or delete records based on matching conditions.

## Usage

``` r
delta_merge(
  table,
  source,
  predicate,
  source_alias = "source",
  target_alias = "target",
  storage_options = NULL
)
```

## Arguments

- table:

  A DeltaTable object or character path to Delta table.

- source:

  Source data (data.frame, Arrow Table, or any nanoarrow-compatible
  object).

- predicate:

  Character. SQL-like predicate for matching (e.g., "target.id =
  source.id").

- source_alias:

  Character. Alias for source table in predicates (default: "source").

- target_alias:

  Character. Alias for target table in predicates (default: "target").

- storage_options:

  Named list. Storage backend options (optional).

## Value

A DeltaMergeBuilder object that can be further configured with
`when_matched_*` and `when_not_matched_*` methods.

## See also

[`when_matched_update`](https://ixpantia.github.io/deltaR/reference/when_matched_update.md),
[`when_matched_delete`](https://ixpantia.github.io/deltaR/reference/when_matched_delete.md),
[`when_not_matched_insert`](https://ixpantia.github.io/deltaR/reference/when_not_matched_insert.md),
[`when_not_matched_by_source_delete`](https://ixpantia.github.io/deltaR/reference/when_not_matched_by_source_delete.md),
[`merge_execute`](https://ixpantia.github.io/deltaR/reference/merge_execute.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Create target table
target <- data.frame(id = 1:3, value = c(10, 20, 30))
write_deltalake(target, "path/to/table")

# Prepare source data for merge
source <- data.frame(id = c(2, 4), value = c(25, 40))

# Perform upsert (update existing, insert new)
result <- delta_merge("path/to/table", source, "target.id = source.id") |>
  when_matched_update(c(value = "source.value")) |>
  when_not_matched_insert(c(id = "source.id", value = "source.value")) |>
  merge_execute()
} # }
```
