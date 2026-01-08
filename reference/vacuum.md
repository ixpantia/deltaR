# Vacuum a Delta table

Remove files no longer referenced by the Delta table and are older than
the retention threshold.

## Usage

``` r
vacuum(table, ...)
```

## Arguments

- table:

  A DeltaTable object.

- retention_hours:

  Numeric. Files older than this will be removed. Default is 168 hours
  (7 days).

- dry_run:

  Logical. If TRUE, only list files that would be removed.

- enforce_retention_duration:

  Logical. If FALSE, allow retention less than the default. Use with
  caution!

## Value

Character vector of files that were (or would be) removed.
