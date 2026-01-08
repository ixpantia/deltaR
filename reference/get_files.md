# Get the list of Parquet files in the current table snapshot

Returns the absolute URIs of all Parquet files that make up the current
version of the Delta table. These can be passed to other tools like
arrow, polars, or duckdb for reading.

## Usage

``` r
get_files(table, ...)
```

## Arguments

- table:

  A DeltaTable object.

## Value

Character vector of file URIs.

## Examples

``` r
if (FALSE) { # \dontrun{
dt <- delta_table("path/to/delta_table")
files <- get_files(dt)

# Use with arrow
arrow::open_dataset(files)
} # }
```
