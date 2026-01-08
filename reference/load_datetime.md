# Load table at a specific datetime

Updates the DeltaTable to the version that was active at the specified
time.

## Usage

``` r
load_datetime(table, ..., datetime)
```

## Arguments

- table:

  A DeltaTable object.

- ...:

  Additional arguments passed to methods.

- datetime:

  Character. ISO 8601 formatted datetime string.

## Value

The DeltaTable object (invisibly), updated to the specified time.
