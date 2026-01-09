# Add WHEN NOT MATCHED BY SOURCE THEN UPDATE clause

Updates target rows that don't have a match in the source data.

## Usage

``` r
when_not_matched_by_source_update(builder, ...)
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

  Named character vector mapping target columns to expressions. Names
  are target column names, values are SQL expressions.

- predicate:

  Optional character. Additional predicate to filter which non-matched
  target rows should be updated.

## Examples

``` r
if (FALSE) { # \dontrun{
# Mark unmatched target rows as inactive
delta_merge(table, source, "target.id = source.id") |>
  when_not_matched_by_source_update(c(active = "false"))
} # }
```
