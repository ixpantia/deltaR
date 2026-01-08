#' @importFrom rlang abort
NULL

#' Check if a path is a local filesystem path (not a cloud URI)
#'
#' @param path Character. Path to check.
#' @return Logical. TRUE if the path is a local filesystem path.
#' @noRd
is_local_path <- function(path) {
  # Cloud storage URIs typically start with a scheme like s3://, gs://, az://, abfs://, etc.

  !grepl("^[a-zA-Z][a-zA-Z0-9+.-]*://", path)
}

#' Ensure directory exists for local paths
#'
#' @param path Character. Path to check/create.
#' @noRd
ensure_directory_exists <- function(path) {
  if (is_local_path(path) && !dir.exists(path)) {
    dir.create(path, recursive = TRUE)
  }
}

#' Write data to a Delta Lake table
#'
#' Writes data to a Delta Lake table, creating it if it doesn't exist.
#'
#' @param data Data to write. Can be a data.frame, Arrow Table, Arrow RecordBatch,
#'   or any object that can be converted to an Arrow RecordBatchReader via
#'   `nanoarrow::as_nanoarrow_array_stream()`.
#' @param table_or_uri Character. Path to the Delta table (local filesystem or cloud storage URI).
#' @param mode Character. How to handle existing data. One of:
#'   \itemize{
#'     \item `"error"` (default): Fail if table exists.
#'     \item `"append"`: Add new data to the table.
#'     \item `"overwrite"`: Replace all data in the table.
#'     \item `"ignore"`: Do nothing if table exists.
#'   }
#' @param partition_by Character vector. Column names to partition by (optional).
#' @param name Character. Table name for metadata (optional, used when creating new table).
#' @param description Character. Table description for metadata (optional).
#' @param storage_options Named list. Storage backend options such as credentials (optional).
#' @param schema_mode Character. How to handle schema evolution (optional). One of:
#'   \itemize{
#'     \item `"overwrite"`: Replace the schema with the new schema.
#'     \item `"merge"`: Merge the new schema with the existing schema.
#'   }
#' @param target_file_size Integer. Target size in bytes for each output file (optional).
#'   When set, the writer will try to create files of approximately this size.
#'
#' @return A list with write result information:
#'   \itemize{
#'     \item `version`: The new version number of the table.
#'     \item `num_files`: Number of files in the table after write.
#'   }
#'
#' @examples
#' \dontrun{
#' # Write a data.frame to a new Delta table
#' df <- data.frame(x = 1:10, y = letters[1:10])
#' write_deltalake(df, "path/to/delta_table")
#'
#' # Append data to an existing table
#' write_deltalake(df, "path/to/delta_table", mode = "append")
#'
#' # Overwrite existing data
#' write_deltalake(df, "path/to/delta_table", mode = "overwrite")
#'
#' # Create a partitioned table
#' write_deltalake(df, "path/to/delta_table", partition_by = "y")
#'
#' # Write to Google Cloud Storage
#' write_deltalake(
#'   df,
#'   "gs://my-bucket/path/to/table",
#'   storage_options = list(google_service_account_path = "path/to/key.json")
#' )
#'
#' # Write to S3
#' write_deltalake(
#'   df,
#'   "s3://my-bucket/path/to/table",
#'   storage_options = list(
#'     aws_access_key_id = "MY_ACCESS_KEY",
#'     aws_secret_access_key = "MY_SECRET_KEY",
#'     aws_region = "us-east-1"
#'   )
#' )
#'
#' # Write to Azure Blob Storage
#' write_deltalake(
#'   df,
#'   "az://my-container/path/to/table",
#'   storage_options = list(
#'     azure_storage_account_name = "MY_ACCOUNT_NAME",
#'     azure_storage_account_key = "MY_ACCOUNT_KEY"
#'   )
#' )
#' }
#'
#' @export
write_deltalake <- function(
  data,
  table_or_uri,
  mode = c("error", "append", "overwrite", "ignore"),
  partition_by = NULL,
  name = NULL,
  description = NULL,
  storage_options = NULL,
  schema_mode = NULL,
  target_file_size = NULL
) {
  # Validate mode

  mode <- match.arg(mode)

  # Validate table_or_uri
  if (!is.character(table_or_uri) || length(table_or_uri) != 1) {
    stop("'table_or_uri' must be a single character string")
  }

  # Create directory if it's a local path and doesn't exist
  ensure_directory_exists(table_or_uri)

  # Convert data to nanoarrow array stream
  stream <- nanoarrow::as_nanoarrow_array_stream(data)

  # Call Rust function

  result <- delta_write(
    table_uri = table_or_uri,
    stream = stream,
    mode = mode,
    partition_by = partition_by,
    name = name,
    description = description,
    storage_options = storage_options,
    schema_mode = schema_mode,
    target_file_size = target_file_size
  )

  # Handle errors from Rust
  if (methods::is(result, "error")) {
    rlang::abort(result$value)
  }

  invisible(result)
}

#' Create a new empty Delta Lake table
#'
#' Creates a new Delta Lake table with the specified schema. The table will be empty
#' after creation.
#'
#' @param table_uri Character. Path where the table will be created (local filesystem or cloud storage URI).
#' @param schema An Arrow schema defining the table structure. Can be created with
#'   `nanoarrow::na_struct()` or `arrow::schema()`.
#' @param partition_by Character vector. Column names to partition by (optional).
#' @param name Character. Table name for metadata (optional).
#' @param description Character. Table description for metadata (optional).
#' @param storage_options Named list. Storage backend options such as credentials (optional).
#' @param configuration Named list. Delta table configuration properties (optional).
#'
#' @return The version number of the created table (typically 0).
#'
#' @examples
#' \dontrun{
#' # Create a table with a simple schema using nanoarrow
#' schema <- nanoarrow::na_struct(list(
#'   id = nanoarrow::na_int64(),
#'   name = nanoarrow::na_string(),
#'   value = nanoarrow::na_double()
#' ))
#' create_deltalake("path/to/new_table", schema)
#'
#' # Create a partitioned table
#' create_deltalake(
#'   "path/to/partitioned_table",
#'   schema,
#'   partition_by = "id",
#'   name = "my_table",
#'   description = "A partitioned Delta table"
#' )
#'
#' # Create a table in Google Cloud Storage
#' create_deltalake(
#'   "gs://my-bucket/path/to/new_table",
#'   schema,
#'   storage_options = list(google_service_account_path = "path/to/key.json")
#' )
#'
#' # Create a table in S3
#' create_deltalake(
#'   "s3://my-bucket/path/to/new_table",
#'   schema,
#'   storage_options = list(
#'     aws_access_key_id = "MY_ACCESS_KEY",
#'     aws_secret_access_key = "MY_SECRET_KEY",
#'     aws_region = "us-east-1"
#'   )
#' )
#'
#' # Create a table in Azure Blob Storage
#' create_deltalake(
#'   "az://my-container/path/to/new_table",
#'   schema,
#'   storage_options = list(
#'     azure_storage_account_name = "MY_ACCOUNT_NAME",
#'     azure_storage_account_key = "MY_ACCOUNT_KEY"
#'   )
#' )
#' }
#'
#' @export
create_deltalake <- function(
  table_uri,
  schema,
  partition_by = NULL,
  name = NULL,
  description = NULL,
  storage_options = NULL,
  configuration = NULL
) {
  # Validate table_uri
  if (!is.character(table_uri) || length(table_uri) != 1) {
    stop("'table_uri' must be a single character string")
  }

  # Create directory if it's a local path and doesn't exist
  ensure_directory_exists(table_uri)

  # Validate schema is not NULL
  if (is.null(schema)) {
    stop("'schema' must be provided and cannot be NULL")
  }

  # Convert schema to nanoarrow if needed
  if (!inherits(schema, "nanoarrow_schema")) {
    # Try to convert from arrow schema or other formats
    schema <- nanoarrow::as_nanoarrow_schema(schema)
  }

  # Call Rust function
  result <- delta_create(
    table_uri = table_uri,
    schema = schema,
    partition_by = partition_by,
    name = name,
    description = description,
    storage_options = storage_options,
    configuration = configuration
  )

  # Handle errors from Rust
  if (methods::is(result, "error")) {
    rlang::abort(result$value)
  }

  invisible(result)
}

#' Compact a Delta table
#'
#' Compact files in a Delta table to reduce the number of small files and
#' improve query performance.
#'
#' @param table A DeltaTable object.
#' @param ... Additional arguments passed to methods.
#' @param target_size Numeric. Target size in bytes for compacted files.
#' @param max_concurrent_tasks Integer. Maximum number of concurrent tasks.
#' @param min_commit_interval_ms Numeric. Minimum interval between commits in milliseconds.
#' @param partition_filters Character vector. Filters to select partitions to compact (e.g., c("date=2023-01-01")).
#'
#' @return A list with compaction metrics.
#'
#' @export
compact <- new_generic(
  "compact",
  "table",
  function(
    table,
    ...,
    target_size = NULL,
    max_concurrent_tasks = NULL,
    min_commit_interval_ms = NULL,
    partition_filters = NULL
  ) {
    S7::S7_dispatch()
  }
)

#' @export
method(compact, DeltaTable) <- function(
  table,
  ...,
  target_size = NULL,
  max_concurrent_tasks = NULL,
  min_commit_interval_ms = NULL,
  partition_filters = NULL
) {
  result <- table@internal$compact(
    target_size,
    if (!is.null(max_concurrent_tasks)) {
      as.integer(max_concurrent_tasks)
    } else {
      NULL
    },
    min_commit_interval_ms,
    partition_filters
  )
  if (methods::is(result, "error")) {
    rlang::abort(result$value)
  }
  result
}
