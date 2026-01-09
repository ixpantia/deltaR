# Add WHEN MATCHED THEN DELETE clause

Deletes target rows that match source rows.

## Usage

``` r
when_matched_delete(builder, ...)
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

  Optional character. Additional predicate to filter which matched rows
  should be deleted.

## Examples

``` r
if (FALSE) { # \dontrun{
# Delete all matched rows
delta_merge(table, source, "target.id = source.id") |>
  when_matched_delete()

# Delete matched rows where a condition is met
delta_merge(table, source, "target.id = source.id") |>
  when_matched_delete(predicate = "source.deleted = true")
} # }
```
