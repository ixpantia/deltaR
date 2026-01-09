# Add WHEN NOT MATCHED BY SOURCE THEN DELETE clause

Deletes target rows that don't have a match in the source data. This is
useful for syncing a target table to exactly match the source.

## Usage

``` r
when_not_matched_by_source_delete(builder, ...)
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

- predicate:

  Optional character. Additional predicate to filter which non-matched
  target rows should be deleted.

## Examples

``` r
if (FALSE) { # \dontrun{
# Delete target rows that are not in source
delta_merge(table, source, "target.id = source.id") |>
  when_not_matched_by_source_delete()

# Delete with a condition
delta_merge(table, source, "target.id = source.id") |>
  when_not_matched_by_source_delete(predicate = "target.deletable = true")
} # }
```
