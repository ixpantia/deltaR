# Get commit history

Returns the commit history of the Delta table.

## Usage

``` r
history(table, ...)
```

## Arguments

- table:

  A DeltaTable object.

- ...:

  Additional arguments passed to methods.

## Value

A data.frame with columns: version, timestamp, operation, user_id,
user_name.
