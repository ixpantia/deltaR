# Get table metadata

Returns metadata about the Delta table including id, name, description,
partition columns, and configuration.

## Usage

``` r
get_metadata(table, ...)
```

## Arguments

- table:

  A DeltaTable object.

- ...:

  Additional arguments passed to methods.

## Value

A named list with table metadata.
