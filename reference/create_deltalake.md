# Create a new empty Delta Lake table

Creates a new Delta Lake table with the specified schema. The table will
be empty after creation.

## Usage

``` r
create_deltalake(
  table_uri,
  schema,
  partition_by = NULL,
  name = NULL,
  description = NULL,
  storage_options = NULL,
  configuration = NULL
)
```

## Arguments

- table_uri:

  Character. Path where the table will be created (local filesystem or
  cloud storage URI).

- schema:

  An Arrow schema defining the table structure. Can be created with
  [`nanoarrow::na_struct()`](https://arrow.apache.org/nanoarrow/latest/r/reference/na_type.html)
  or
  [`arrow::schema()`](https://arrow.apache.org/docs/r/reference/schema.html).

- partition_by:

  Character vector. Column names to partition by (optional).

- name:

  Character. Table name for metadata (optional).

- description:

  Character. Table description for metadata (optional).

- storage_options:

  Named list. Storage backend options such as credentials (optional).

- configuration:

  Named list. Delta table configuration properties (optional).

## Value

The version number of the created table (typically 0).

## Examples

``` r
if (FALSE) { # \dontrun{
# Create a table with a simple schema using nanoarrow
schema <- nanoarrow::na_struct(list(
  id = nanoarrow::na_int64(),
  name = nanoarrow::na_string(),
  value = nanoarrow::na_double()
))
create_deltalake("path/to/new_table", schema)

# Create a partitioned table
create_deltalake(
  "path/to/partitioned_table",
  schema,
  partition_by = "id",
  name = "my_table",
  description = "A partitioned Delta table"
)

# Create a table in Google Cloud Storage
create_deltalake(
  "gs://my-bucket/path/to/new_table",
  schema,
  storage_options = list(google_service_account_path = "path/to/key.json")
)

# Create a table in S3
create_deltalake(
  "s3://my-bucket/path/to/new_table",
  schema,
  storage_options = list(
    aws_access_key_id = "MY_ACCESS_KEY",
    aws_secret_access_key = "MY_SECRET_KEY",
    aws_region = "us-east-1"
  )
)

# Create a table in Azure Blob Storage
create_deltalake(
  "az://my-container/path/to/new_table",
  schema,
  storage_options = list(
    azure_storage_account_name = "MY_ACCOUNT_NAME",
    azure_storage_account_key = "MY_ACCOUNT_KEY"
  )
)
} # }
```
