# Add WHEN NOT MATCHED THEN INSERT ALL clause

Inserts all columns for source rows that don't match any target rows.
Requires source and target to have matching column names.

## Usage

``` r
when_not_matched_insert_all(builder, ...)
```

## Arguments

- builder:

  A DeltaMergeBuilder object.

- ...:

  Additional arguments passed to methods.

## Value

The modified DeltaMergeBuilder (for method chaining).

## Note

This function is not yet fully supported and will error on execution.
Please use
[`when_not_matched_insert`](https://ixpantia.github.io/deltaR/reference/when_not_matched_insert.md)
with explicit column mappings instead.

## Method arguments

The method for DeltaMergeBuilder accepts:

- predicate:

  Optional character. Additional predicate to filter which non-matched
  source rows should be inserted.

## Examples

``` r
if (FALSE) { # \dontrun{
# Insert all columns for non-matched source rows (NOT YET SUPPORTED)
# Use when_not_matched_insert with explicit columns instead:
delta_merge(table, source, "target.id = source.id") |>
  when_not_matched_insert(c(col1 = "source.col1", col2 = "source.col2"))
} # }
```
