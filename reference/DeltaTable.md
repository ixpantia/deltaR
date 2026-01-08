# DeltaTable S7 Class

An S7 class representing a Delta Lake table.

## Usage

``` r
DeltaTable(path = "", internal = NULL)
```

## Arguments

- path:

  Character. The path to the Delta table (local or cloud storage URI).

- internal:

  The internal Rust DeltaTableInternal object.
