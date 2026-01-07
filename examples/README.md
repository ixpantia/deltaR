# deltaR Examples

This folder contains example scripts demonstrating how to use the `deltaR` package.

## Files

### `quickstart.R`

A minimal quick-start guide showing the most common operations:

- Writing a data.frame to a Delta table
- Opening and exploring tables
- Appending and overwriting data
- Time travel (loading previous versions)
- Creating empty tables with schemas
- Partitioned tables

**Best for:** Getting started quickly with `deltaR`.

### `deltaR_example.R`

A comprehensive example script covering all major features:

1. Writing data.frames to new Delta tables
2. Opening and exploring Delta tables
3. Appending data to existing tables
4. Overwriting data in tables
5. Time travel - loading previous versions
6. Creating empty tables with schemas
7. Creating partitioned tables
8. Integration with other R tools (arrow, polars, duckdb)
9. Vacuum operations
10. Checking if paths are Delta tables

**Best for:** Learning all available features in depth.

### `gcs_example.R`

Demonstrates interaction with Google Cloud Storage:

- Configuring storage options for GCS
- Creating tables in a GCS bucket
- Appending data to remote tables
- Managing credentials using environment variables

**Best for:** Users needing to store data in Google Cloud.

## Running the Examples

```r
# Run the quick start example
source("quickstart.R")

# Run the comprehensive example
source("deltaR_example.R")
```

## Requirements

- `deltaR` package installed
- `nanoarrow` package (automatically installed with `deltaR`)

Optional:
- `arrow` package for reading Parquet files directly
