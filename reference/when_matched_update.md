# Add WHEN MATCHED THEN UPDATE clause

Specifies how to update target rows when they match source rows.

## Usage

``` r
when_matched_update(builder, ...)
```

## Arguments

- builder:

  A DeltaMergeBuilder object.

- ...:

  Additional arguments passed to methods.

## Value

The modified DeltaMergeBuilder (for method chaining).

## Method arguments

The method for DeltaMergeBuilder accepts:

- updates:

  Named character vector mapping target columns to source expressions.
  Names are target column names, values are SQL expressions (e.g.,
  "source.column").

- predicate:

  Optional character. Additional predicate to filter which matched rows
  should be updated.

## Examples

``` r
if (FALSE) { # \dontrun{
# Update all matched rows
delta_merge(table, source, "target.id = source.id") |>
  when_matched_update(c(value = "source.value", name = "source.name"))

# Update only matched rows where a condition is met
delta_merge(table, source, "target.id = source.id") |>
  when_matched_update(
    c(value = "source.value"),
    predicate = "source.value > target.value"
  )
} # }
```
