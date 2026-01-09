# Add WHEN MATCHED THEN UPDATE ALL clause

Updates all columns in matched target rows with corresponding source
values. Requires source and target to have matching column names.

## Usage

``` r
when_matched_update_all(builder, ...)
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
[`when_matched_update`](https://ixpantia.github.io/deltaR/reference/when_matched_update.md)
with explicit column mappings instead.

## Method arguments

The method for DeltaMergeBuilder accepts:

- predicate:

  Optional character. Additional predicate to filter which matched rows
  should be updated.

## Examples

``` r
if (FALSE) { # \dontrun{
# Update all columns for matched rows (NOT YET SUPPORTED)
# Use when_matched_update with explicit columns instead:
delta_merge(table, source, "target.id = source.id") |>
  when_matched_update(c(col1 = "source.col1", col2 = "source.col2"))
} # }
```
