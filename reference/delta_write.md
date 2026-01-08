# Write data to a Delta Lake table using WriteBuilder and LogicalPlan

This function uses DataFusion's execution framework to write data,
providing:

- Better error handling through the DataFusion pipeline

- Lazy schema casting (casts each batch on-the-fly)

- Proper backpressure handling for large datasets

- Memory-efficient streaming writes

## Usage

``` r
delta_write(
  table_uri,
  stream,
  mode,
  partition_by,
  name,
  description,
  storage_options,
  schema_mode,
  target_file_size
)
```

## Arguments

- table_uri:

  Path to the Delta table (will be created if it doesn't exist)

- mode:

  Save mode: "append", "overwrite", "error", or "ignore"

- partition_by:

  Column names to partition by (optional)

- name:

  Table name (optional, used when creating new table)

- description:

  Table description (optional, used when creating new table)

- storage_options:

  Storage backend options (optional)

- schema_mode:

  How to handle schema evolution: "overwrite" or "merge" (optional)

- target_file_size:

  Target file size in bytes (optional)

- data:

  Arrow data stream (nanoarrow_array_stream)
