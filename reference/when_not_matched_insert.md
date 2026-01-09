# Add WHEN NOT MATCHED THEN INSERT clause

Inserts source rows that don't match any target rows.

## Usage

``` r
when_not_matched_insert(builder, ...)
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

  Optional character. Additional predicate to filter which non-matched
  source rows should be inserted.

## Examples

``` r
if (FALSE) { # \dontrun{
# Insert non-matched source rows
delta_merge(table, source, "target.id = source.id") |>
  when_not_matched_insert(c(id = "source.id", value = "source.value"))

# Insert with a condition
delta_merge(table, source, "target.id = source.id") |>
  when_not_matched_insert(
    c(id = "source.id", value = "source.value"),
    predicate = "source.value IS NOT NULL"
  )
} # }
```
