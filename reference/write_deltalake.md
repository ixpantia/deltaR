# Write data to a Delta Lake table

Writes data to a Delta Lake table, creating it if it doesn't exist.

## Usage

``` r
write_deltalake(
  data,
  table_or_uri,
  mode = c("error", "append", "overwrite", "ignore"),
  partition_by = NULL,
  name = NULL,
  description = NULL,
  storage_options = NULL,
  schema_mode = NULL,
  target_file_size = NULL
)
```

## Arguments

- data:

  Data to write. Can be a data.frame, Arrow Table, Arrow RecordBatch, or
  any object that can be converted to an Arrow RecordBatchReader via
  [`nanoarrow::as_nanoarrow_array_stream()`](https://arrow.apache.org/nanoarrow/latest/r/reference/as_nanoarrow_array_stream.html).

- table_or_uri:

  Character. Path to the Delta table (local filesystem or cloud storage
  URI).

- mode:

  Character. How to handle existing data. One of:

  - `"error"` (default): Fail if table exists.

  - `"append"`: Add new data to the table.

  - `"overwrite"`: Replace all data in the table.

  - `"ignore"`: Do nothing if table exists.

- partition_by:

  Character vector. Column names to partition by (optional).

- name:

  Character. Table name for metadata (optional, used when creating new
  table).

- description:

  Character. Table description for metadata (optional).

- storage_options:

  Named list. Storage backend options such as credentials (optional).

- schema_mode:

  Character. How to handle schema evolution (optional). One of:

  - `"overwrite"`: Replace the schema with the new schema.

  - `"merge"`: Merge the new schema with the existing schema.

- target_file_size:

  Integer. Target size in bytes for each output file (optional). When
  set, the writer will try to create files of approximately this size.

## Value

A list with write result information:

- `version`: The new version number of the table.

- `num_files`: Number of files in the table after write.

## Examples

``` r
if (FALSE) { # \dontrun{
# Write a data.frame to a new Delta table
df <- data.frame(x = 1:10, y = letters[1:10])
write_deltalake(df, "path/to/delta_table")

# Append data to an existing table
write_deltalake(df, "path/to/delta_table", mode = "append")

# Overwrite existing data
write_deltalake(df, "path/to/delta_table", mode = "overwrite")

# Create a partitioned table
write_deltalake(df, "path/to/delta_table", partition_by = "y")

# Write to Google Cloud Storage
write_deltalake(
  df,
  "gs://my-bucket/path/to/table",
  storage_options = list(google_service_account_path = "path/to/key.json")
)

# Write to S3
write_deltalake(
  df,
  "s3://my-bucket/path/to/table",
  storage_options = list(
    aws_access_key_id = "MY_ACCESS_KEY",
    aws_secret_access_key = "MY_SECRET_KEY",
    aws_region = "us-east-1"
  )
)

# Write to Azure Blob Storage
write_deltalake(
  df,
  "az://my-container/path/to/table",
  storage_options = list(
    azure_storage_account_name = "MY_ACCOUNT_NAME",
    azure_storage_account_key = "MY_ACCOUNT_KEY"
  )
)
} # }
```
