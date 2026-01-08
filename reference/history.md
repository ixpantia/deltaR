# Get commit history

Returns the commit history of the Delta table.

## Usage

``` r
history(table, ..., limit = NULL)
```

## Arguments

- table:

  A DeltaTable object.

- ...:

  Additional arguments passed to methods.

- limit:

  The maximum number of commits to return.

## Value

A data.frame with columns: version, timestamp, operation, user_id,
user_name.
