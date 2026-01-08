#' @importFrom rlang abort
NULL

#' Create a DeltaTable object
#'
#' Opens an existing Delta Lake table at the specified path.
#'
#' @param path Character. Path to the Delta table (local filesystem or cloud storage URI).
#' @param version Optional integer. Load a specific version of the table.
#' @param datetime Optional character. Load the table at a specific point in time (ISO 8601 format).
#' @param storage_options Optional named list. Storage backend options (e.g., AWS credentials).
#'
#' @return A DeltaTable S7 object.
#'
#' @examples
#' \dontrun{
#' # Open a local Delta table
#' dt <- delta_table("path/to/delta_table")
#'
#' # Open at a specific version
#' dt <- delta_table("path/to/delta_table", version = 5)
#'
#' # Open at a specific datetime
#' dt <- delta_table("path/to/delta_table", datetime = "2024-01-01T00:00:00Z")
#'
#' # Open a Google Cloud Storage Delta table
#' dt <- delta_table(
#'   "gs://my-bucket/path/to/table",
#'   storage_options = list(google_service_account_path = "path/to/key.json")
#' )
#'
#' # Open an S3 Delta table
#' dt <- delta_table(
#'   "s3://my-bucket/path/to/table",
#'   storage_options = list(
#'     aws_access_key_id = "MY_ACCESS_KEY",
#'     aws_secret_access_key = "MY_SECRET_KEY",
#'     aws_region = "us-east-1"
#'   )
#' )
#'
#' # Open an Azure Blob Storage Delta table
#' dt <- delta_table(
#'   "az://my-container/path/to/table",
#'   storage_options = list(
#'     azure_storage_account_name = "MY_ACCOUNT_NAME",
#'     azure_storage_account_key = "MY_ACCOUNT_KEY"
#'   )
#' )
#' }
#'
#' @export
delta_table <- function(
  path,
  version = NULL,
  datetime = NULL,
  storage_options = NULL
) {
  # Validate inputs
  if (!is.character(path) || length(path) != 1) {
    stop("'path' must be a single character string")
  }

  if (!is.null(version) && !is.null(datetime)) {
    stop("Cannot specify both 'version' and 'datetime'")
  }

  # Open the table
  internal <- delta_table_open(path, storage_options)

  # Handle errors from Rust
  if (methods::is(internal, "error")) {
    rlang::abort(internal$value)
  }

  # Time travel if requested
  if (!is.null(version)) {
    result <- internal$load_version(as.integer(version))
    if (methods::is(result, "error")) {
      rlang::abort(result$value)
    }
  } else if (!is.null(datetime)) {
    result <- internal$load_datetime(datetime)
    if (methods::is(result, "error")) {
      rlang::abort(result$value)
    }
  }

  # Create and return the S7 object
  DeltaTable(path = path, internal = internal)
}

#' Get the current version of a Delta table
#'
#' @param table A DeltaTable object.
#' @param ... Additional arguments passed to methods.
#'
#' @return Integer. The current version number.
#'
#' @export
table_version <- new_generic("table_version", "table", function(table, ...) {
  S7::S7_dispatch()
})

#' @export
method(table_version, DeltaTable) <- function(table, ...) {
  result <- table@internal$version()
  if (methods::is(result, "error")) {
    rlang::abort(result$value)
  }
  result
}

#' Get the list of Parquet files in the current table snapshot
#'
#' Returns the absolute URIs of all Parquet files that make up the current
#' version of the Delta table. These can be passed to other tools like
#' arrow, polars, or duckdb for reading.
#'
#' @param table A DeltaTable object.
#' @param ... Additional arguments passed to methods.
#'
#' @return Character vector of file URIs.
#'
#' @examples
#' \dontrun{
#' dt <- delta_table("path/to/delta_table")
#' files <- get_files(dt)
#'
#' # Use with arrow
#' arrow::open_dataset(files)
#' }
#'
#' @export
get_files <- new_generic("get_files", "table", function(table, ...) {
  S7::S7_dispatch()
})

#' @export
method(get_files, DeltaTable) <- function(table) {
  result <- table@internal$get_files()
  if (methods::is(result, "error")) {
    rlang::abort(result$value)
  }
  result
}

#' Get table metadata
#'
#' Returns metadata about the Delta table including id, name, description,
#' partition columns, and configuration.
#'
#' @param table A DeltaTable object.
#' @param ... Additional arguments passed to methods.
#'
#' @return A named list with table metadata.
#'
#' @export
get_metadata <- new_generic("get_metadata", "table", function(table, ...) {
  S7::S7_dispatch()
})

#' @export
method(get_metadata, DeltaTable) <- function(table) {
  result <- table@internal$metadata()
  if (methods::is(result, "error")) {
    rlang::abort(result$value)
  }
  result
}

#' Get table schema
#'
#' Returns the Arrow schema of the Delta table.
#'
#' @param table A DeltaTable object.
#' @param ... Additional arguments passed to methods.
#'
#' @return An Arrow Schema object.
#'
#' @export
get_schema <- new_generic("get_schema", "table", function(table, ...) {
  S7::S7_dispatch()
})

#' @export
method(get_schema, DeltaTable) <- function(table) {
  result <- table@internal$schema()
  if (methods::is(result, "error")) {
    rlang::abort(result$value)
  }
  result
}

#' Get commit history
#'
#' Returns the commit history of the Delta table.
#'
#' @param table A DeltaTable object.
#' @param ... Additional arguments passed to methods.
#'
#' @return A data.frame with columns: version, timestamp, operation, user_id, user_name.
#'
#' @export
history <- new_generic("history", "table", function(table, ..., limit = NULL) {
  S7::S7_dispatch()
})

#' @export
method(history, DeltaTable) <- function(table, ..., limit = NULL) {
  result <- table@internal$history(limit)
  if (methods::is(result, "error")) {
    rlang::abort(result$value)
  }
  result
}

#' Get partition columns
#'
#' Returns the partition columns of the Delta table.
#'
#' @param table A DeltaTable object.
#' @param ... Additional arguments passed to methods.
#'
#' @return Character vector of partition column names.
#'
#' @export
partition_columns <- new_generic(
  "partition_columns",
  "table",
  function(table, ...) {
    S7::S7_dispatch()
  }
)

#' @export
method(partition_columns, DeltaTable) <- function(table) {
  result <- table@internal$partition_columns()
  if (methods::is(result, "error")) {
    rlang::abort(result$value)
  }
  result
}

#' Vacuum a Delta table
#'
#' Remove files no longer referenced by the Delta table and are older than
#' the retention threshold.
#'
#' @param table A DeltaTable object.
#' @param ... Additional arguments passed to methods.
#' @param retention_hours Numeric. Files older than this will be removed.
#'   Default is 168 hours (7 days).
#' @param dry_run Logical. If TRUE, only list files that would be removed.
#' @param enforce_retention_duration Logical. If FALSE, allow retention less
#'   than the default. Use with caution!
#'
#' @return Character vector of files that were (or would be) removed.
#'
#' @export
vacuum <- new_generic(
  "vacuum",
  "table",
  function(
    table,
    ...,
    retention_hours = NULL,
    dry_run = TRUE,
    enforce_retention_duration = TRUE
  ) {
    S7::S7_dispatch()
  }
)

#' @export
method(vacuum, DeltaTable) <- function(
  table,
  ...,
  retention_hours = NULL,
  dry_run = TRUE,
  enforce_retention_duration = TRUE
) {
  result <- table@internal$vacuum(
    retention_hours,
    dry_run,
    enforce_retention_duration
  )
  if (methods::is(result, "error")) {
    rlang::abort(result$value)
  }
  result
}

#' Load a specific version of the table
#'
#' Updates the DeltaTable to point to a specific version.
#'
#' @param table A DeltaTable object.
#' @param ... Additional arguments passed to methods.
#' @param version Integer. The version number to load.
#'
#' @return The DeltaTable object (invisibly), updated to the specified version.
#'
#' @export
load_version <- new_generic(
  "load_version",
  "table",
  function(table, ..., version) {
    S7::S7_dispatch()
  }
)

#' @export
method(load_version, DeltaTable) <- function(table, ..., version) {
  result <- table@internal$load_version(as.integer(version))
  if (methods::is(result, "error")) {
    rlang::abort(result$value)
  }
  invisible(table)
}

#' Load table at a specific datetime
#'
#' Updates the DeltaTable to the version that was active at the specified time.
#'
#' @param table A DeltaTable object.
#' @param ... Additional arguments passed to methods.
#' @param datetime Character. ISO 8601 formatted datetime string.
#'
#' @return The DeltaTable object (invisibly), updated to the specified time.
#'
#' @export
load_datetime <- new_generic(
  "load_datetime",
  "table",
  function(table, ..., datetime) {
    S7::S7_dispatch()
  }
)

#' @export
method(load_datetime, DeltaTable) <- function(table, ..., datetime) {
  result <- table@internal$load_datetime(datetime)
  if (methods::is(result, "error")) {
    rlang::abort(result$value)
  }
  invisible(table)
}


#' Check if a path contains a Delta table
#'
#' @param path Character. Path to check.
#' @param storage_options Optional named list. Storage backend options.
#'
#' @return Logical. TRUE if the path contains a valid Delta table.
#'
#' @export
is_delta_table_path <- function(path, storage_options = NULL) {
  result <- is_delta_table(path, storage_options)
  if (methods::is(result, "error")) {
    rlang::abort(result$value)
  }
  result
}
