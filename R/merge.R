#' @importFrom rlang abort
#' @importFrom methods is
#' @include 00_classes.R
NULL

# ==============================================================================
# DeltaMergeBuilder S7 Class
# ==============================================================================

#' DeltaMergeBuilder S7 Class
#'
#' An S7 class representing a Delta Lake merge operation builder.
#' All configuration is stored in R and only passed to Rust on execute().
#'
#' This class is typically created by calling \code{\link{delta_merge}} rather
#' than constructing it directly.
#'
#' @param table_path Character. Path to the Delta table.
#' @param storage_options Named list or NULL. Storage backend options.
#' @param source_data The source data for the merge operation.
#' @param predicate Character. SQL-like predicate for matching rows.
#' @param source_alias Character. Alias for source table in expressions.
#' @param target_alias Character. Alias for target table in expressions.
#' @param matched_update_clauses List. WHEN MATCHED UPDATE clauses.
#' @param matched_delete_clauses List. WHEN MATCHED DELETE clauses.
#' @param not_matched_insert_clauses List. WHEN NOT MATCHED INSERT clauses.
#' @param not_matched_by_source_update_clauses List. WHEN NOT MATCHED BY SOURCE UPDATE clauses.
#' @param not_matched_by_source_delete_clauses List. WHEN NOT MATCHED BY SOURCE DELETE clauses.
#'
#' @seealso \code{\link{delta_merge}} for creating merge operations.
#'
#' @export
DeltaMergeBuilder <- new_class(
  "DeltaMergeBuilder",
  properties = list(
    # Target table info
    table_path = new_property(class_character),
    storage_options = new_property(class_list | NULL, default = NULL),

    # Source data (kept as-is, converted to Arrow stream on execute)
    source_data = new_property(class_any),

    # Merge configuration
    predicate = new_property(class_character),
    source_alias = new_property(class_character, default = "source"),
    target_alias = new_property(class_character, default = "target"),

    # Clause lists - each element is a list with 'predicate' and/or 'updates'
    matched_update_clauses = new_property(class_list, default = list()),
    matched_delete_clauses = new_property(class_list, default = list()),
    not_matched_insert_clauses = new_property(class_list, default = list()),
    not_matched_by_source_update_clauses = new_property(
      class_list,
      default = list()
    ),
    not_matched_by_source_delete_clauses = new_property(
      class_list,
      default = list()
    )
  )
)

# Print method for DeltaMergeBuilder
method(print, DeltaMergeBuilder) <- function(x, ...) {
  cat("DeltaMergeBuilder\n")
  cat("  Table:", x@table_path, "\n")
  cat("  Predicate:", x@predicate, "\n")
  cat("  Source alias:", x@source_alias, "\n")
  cat("  Target alias:", x@target_alias, "\n")
  cat("  Clauses:\n")
  cat("    - matched_update:", length(x@matched_update_clauses), "\n")
  cat("    - matched_delete:", length(x@matched_delete_clauses), "\n")
  cat("    - not_matched_insert:", length(x@not_matched_insert_clauses), "\n")
  cat(
    "    - not_matched_by_source_update:",
    length(x@not_matched_by_source_update_clauses),
    "\n"
  )
  cat(
    "    - not_matched_by_source_delete:",
    length(x@not_matched_by_source_delete_clauses),
    "\n"
  )
  invisible(x)
}

# ==============================================================================
# Main Entry Point
# ==============================================================================

#' Start a Delta Lake MERGE operation
#'
#' Creates a DeltaMergeBuilder to configure and execute a MERGE operation
#' that can update, insert, or delete records based on matching conditions.
#'
#' @param table A DeltaTable object or character path to Delta table.
#' @param source Source data (data.frame, Arrow Table, or any nanoarrow-compatible object).
#' @param predicate Character. SQL-like predicate for matching (e.g., "target.id = source.id").
#' @param source_alias Character. Alias for source table in predicates (default: "source").
#' @param target_alias Character. Alias for target table in predicates (default: "target").
#' @param storage_options Named list. Storage backend options (optional).
#'
#' @return A DeltaMergeBuilder object that can be further configured with
#'   `when_matched_*` and `when_not_matched_*` methods.
#'
#' @examples
#' \dontrun{
#' # Create target table
#' target <- data.frame(id = 1:3, value = c(10, 20, 30))
#' write_deltalake(target, "path/to/table")
#'
#' # Prepare source data for merge
#' source <- data.frame(id = c(2, 4), value = c(25, 40))
#'
#' # Perform upsert (update existing, insert new)
#' result <- delta_merge("path/to/table", source, "target.id = source.id") |>
#'   when_matched_update(c(value = "source.value")) |>
#'   when_not_matched_insert(c(id = "source.id", value = "source.value")) |>
#'   merge_execute()
#' }
#'
#' @seealso
#' \code{\link{when_matched_update}}, \code{\link{when_matched_delete}},
#' \code{\link{when_not_matched_insert}}, \code{\link{when_not_matched_by_source_delete}},
#' \code{\link{merge_execute}}
#'
#' @export
delta_merge <- function(
  table,
  source,
  predicate,
  source_alias = "source",
  target_alias = "target",
  storage_options = NULL
) {
  # Validate inputs
  if (
    !is.character(predicate) || length(predicate) != 1 || nchar(predicate) == 0
  ) {
    stop("'predicate' must be a non-empty character string")
  }

  if (!is.character(source_alias) || length(source_alias) != 1) {
    stop("'source_alias' must be a single character string")
  }

  if (!is.character(target_alias) || length(target_alias) != 1) {
    stop("'target_alias' must be a single character string")
  }

  # Extract path from DeltaTable or use as-is
  # S7 classes use S7_class() for type checking
  table_path <- if (S7::S7_inherits(table, DeltaTable)) {
    table@path
  } else if (is.character(table) && length(table) == 1) {
    table
  } else {
    stop("'table' must be a DeltaTable object or a single character path")
  }

  DeltaMergeBuilder(
    table_path = table_path,
    storage_options = storage_options,
    source_data = source,
    predicate = predicate,
    source_alias = source_alias,
    target_alias = target_alias
  )
}

# ==============================================================================
# WHEN MATCHED Clauses
# ==============================================================================

#' Add WHEN MATCHED THEN UPDATE clause
#'
#' Specifies how to update target rows when they match source rows.
#'
#' @param builder A DeltaMergeBuilder object.
#' @param ... Additional arguments passed to methods.
#'
#' @section Method arguments:
#' The method for DeltaMergeBuilder accepts:
#' \describe{
#'   \item{updates}{Named character vector mapping target columns to source expressions.
#'     Names are target column names, values are SQL expressions (e.g., "source.column").}
#'   \item{predicate}{Optional character. Additional predicate to filter which matched
#'     rows should be updated.}
#' }
#'
#' @return The modified DeltaMergeBuilder (for method chaining).
#'
#' @examples
#' \dontrun{
#' # Update all matched rows
#' delta_merge(table, source, "target.id = source.id") |>
#'   when_matched_update(c(value = "source.value", name = "source.name"))
#'
#' # Update only matched rows where a condition is met
#' delta_merge(table, source, "target.id = source.id") |>
#'   when_matched_update(
#'     c(value = "source.value"),
#'     predicate = "source.value > target.value"
#'   )
#' }
#'
#' @export
when_matched_update <- new_generic(
  "when_matched_update",
  "builder",
  function(builder, ...) {
    S7::S7_dispatch()
  }
)

method(when_matched_update, DeltaMergeBuilder) <- function(
  builder,
  updates,
  predicate = NULL
) {
  if (is.null(updates) || length(updates) == 0) {
    stop("'updates' must be a non-empty named vector")
  }

  if (is.null(names(updates)) || any(names(updates) == "")) {
    stop("'updates' must be a named vector with column names")
  }

  clause <- list(updates = as.list(updates))
  if (!is.null(predicate)) {
    if (!is.character(predicate) || length(predicate) != 1) {
      stop("'predicate' must be a single character string")
    }
    clause$predicate <- predicate
  }

  builder@matched_update_clauses <- c(
    builder@matched_update_clauses,
    list(clause)
  )
  builder
}

#' Add WHEN MATCHED THEN UPDATE ALL clause
#'
#' Updates all columns in matched target rows with corresponding source values.
#' Requires source and target to have matching column names.
#'
#' @param builder A DeltaMergeBuilder object.
#' @param ... Additional arguments passed to methods.
#'
#' @section Method arguments:
#' The method for DeltaMergeBuilder accepts:
#' \describe{
#'   \item{predicate}{Optional character. Additional predicate to filter which matched
#'     rows should be updated.}
#' }
#'
#' @return The modified DeltaMergeBuilder (for method chaining).
#'
#' @note This function is not yet fully supported and will error on execution.
#'   Please use \code{\link{when_matched_update}} with explicit column mappings instead.
#'
#' @examples
#' \dontrun{
#' # Update all columns for matched rows (NOT YET SUPPORTED)
#' # Use when_matched_update with explicit columns instead:
#' delta_merge(table, source, "target.id = source.id") |>
#'   when_matched_update(c(col1 = "source.col1", col2 = "source.col2"))
#' }
#'
#' @export
when_matched_update_all <- new_generic(
  "when_matched_update_all",
  "builder",
  function(builder, ...) {
    S7::S7_dispatch()
  }
)

method(when_matched_update_all, DeltaMergeBuilder) <- function(
  builder,
  predicate = NULL
) {
  warning(
    "when_matched_update_all() is not yet fully supported. ",
    "Please use when_matched_update() with explicit column mappings instead.",
    call. = FALSE
  )

  clause <- list(update_all = TRUE)
  if (!is.null(predicate)) {
    if (!is.character(predicate) || length(predicate) != 1) {
      stop("'predicate' must be a single character string")
    }
    clause$predicate <- predicate
  }

  builder@matched_update_clauses <- c(
    builder@matched_update_clauses,
    list(clause)
  )
  builder
}

#' Add WHEN MATCHED THEN DELETE clause
#'
#' Deletes target rows that match source rows.
#'
#' @param builder A DeltaMergeBuilder object.
#' @param ... Additional arguments passed to methods.
#'
#' @section Method arguments:
#' The method for DeltaMergeBuilder accepts:
#' \describe{
#'   \item{predicate}{Optional character. Additional predicate to filter which matched
#'     rows should be deleted.}
#' }
#'
#' @return The modified DeltaMergeBuilder (for method chaining).
#'
#' @examples
#' \dontrun{
#' # Delete all matched rows
#' delta_merge(table, source, "target.id = source.id") |>
#'   when_matched_delete()
#'
#' # Delete matched rows where a condition is met
#' delta_merge(table, source, "target.id = source.id") |>
#'   when_matched_delete(predicate = "source.deleted = true")
#' }
#'
#' @export
when_matched_delete <- new_generic(
  "when_matched_delete",
  "builder",
  function(builder, ...) {
    S7::S7_dispatch()
  }
)

method(when_matched_delete, DeltaMergeBuilder) <- function(
  builder,
  predicate = NULL
) {
  clause <- list()
  if (!is.null(predicate)) {
    if (!is.character(predicate) || length(predicate) != 1) {
      stop("'predicate' must be a single character string")
    }
    clause$predicate <- predicate
  }

  builder@matched_delete_clauses <- c(
    builder@matched_delete_clauses,
    list(clause)
  )
  builder
}

# ==============================================================================
# WHEN NOT MATCHED Clauses (source rows not in target)
# ==============================================================================

#' Add WHEN NOT MATCHED THEN INSERT clause
#'
#' Inserts source rows that don't match any target rows.
#'
#' @param builder A DeltaMergeBuilder object.
#' @param ... Additional arguments passed to methods.
#'
#' @section Method arguments:
#' The method for DeltaMergeBuilder accepts:
#' \describe{
#'   \item{updates}{Named character vector mapping target columns to source expressions.
#'     Names are target column names, values are SQL expressions (e.g., "source.column").}
#'   \item{predicate}{Optional character. Additional predicate to filter which non-matched
#'     source rows should be inserted.}
#' }
#'
#' @return The modified DeltaMergeBuilder (for method chaining).
#'
#' @examples
#' \dontrun{
#' # Insert non-matched source rows
#' delta_merge(table, source, "target.id = source.id") |>
#'   when_not_matched_insert(c(id = "source.id", value = "source.value"))
#'
#' # Insert with a condition
#' delta_merge(table, source, "target.id = source.id") |>
#'   when_not_matched_insert(
#'     c(id = "source.id", value = "source.value"),
#'     predicate = "source.value IS NOT NULL"
#'   )
#' }
#'
#' @export
when_not_matched_insert <- new_generic(
  "when_not_matched_insert",
  "builder",
  function(builder, ...) {
    S7::S7_dispatch()
  }
)

method(when_not_matched_insert, DeltaMergeBuilder) <- function(
  builder,
  updates,
  predicate = NULL
) {
  if (is.null(updates) || length(updates) == 0) {
    stop("'updates' must be a non-empty named vector")
  }

  if (is.null(names(updates)) || any(names(updates) == "")) {
    stop("'updates' must be a named vector with column names")
  }

  clause <- list(updates = as.list(updates))
  if (!is.null(predicate)) {
    if (!is.character(predicate) || length(predicate) != 1) {
      stop("'predicate' must be a single character string")
    }
    clause$predicate <- predicate
  }

  builder@not_matched_insert_clauses <- c(
    builder@not_matched_insert_clauses,
    list(clause)
  )
  builder
}

#' Add WHEN NOT MATCHED THEN INSERT ALL clause
#'
#' Inserts all columns for source rows that don't match any target rows.
#' Requires source and target to have matching column names.
#'
#' @param builder A DeltaMergeBuilder object.
#' @param ... Additional arguments passed to methods.
#'
#' @section Method arguments:
#' The method for DeltaMergeBuilder accepts:
#' \describe{
#'   \item{predicate}{Optional character. Additional predicate to filter which non-matched
#'     source rows should be inserted.}
#' }
#'
#' @return The modified DeltaMergeBuilder (for method chaining).
#'
#' @note This function is not yet fully supported and will error on execution.
#'   Please use \code{\link{when_not_matched_insert}} with explicit column mappings instead.
#'
#' @examples
#' \dontrun{
#' # Insert all columns for non-matched source rows (NOT YET SUPPORTED)
#' # Use when_not_matched_insert with explicit columns instead:
#' delta_merge(table, source, "target.id = source.id") |>
#'   when_not_matched_insert(c(col1 = "source.col1", col2 = "source.col2"))
#' }
#'
#' @export
when_not_matched_insert_all <- new_generic(
  "when_not_matched_insert_all",
  "builder",
  function(builder, ...) {
    S7::S7_dispatch()
  }
)

method(when_not_matched_insert_all, DeltaMergeBuilder) <- function(
  builder,
  predicate = NULL
) {
  warning(
    "when_not_matched_insert_all() is not yet fully supported. ",
    "Please use when_not_matched_insert() with explicit column mappings instead.",
    call. = FALSE
  )

  clause <- list(insert_all = TRUE)
  if (!is.null(predicate)) {
    if (!is.character(predicate) || length(predicate) != 1) {
      stop("'predicate' must be a single character string")
    }
    clause$predicate <- predicate
  }

  builder@not_matched_insert_clauses <- c(
    builder@not_matched_insert_clauses,
    list(clause)
  )
  builder
}

# ==============================================================================
# WHEN NOT MATCHED BY SOURCE Clauses (target rows not in source)
# ==============================================================================

#' Add WHEN NOT MATCHED BY SOURCE THEN UPDATE clause
#'
#' Updates target rows that don't have a match in the source data.
#'
#' @param builder A DeltaMergeBuilder object.
#' @param ... Additional arguments passed to methods.
#'
#' @section Method arguments:
#' The method for DeltaMergeBuilder accepts:
#' \describe{
#'   \item{updates}{Named character vector mapping target columns to expressions.
#'     Names are target column names, values are SQL expressions.}
#'   \item{predicate}{Optional character. Additional predicate to filter which non-matched
#'     target rows should be updated.}
#' }
#'
#' @return The modified DeltaMergeBuilder (for method chaining).
#'
#' @examples
#' \dontrun{
#' # Mark unmatched target rows as inactive
#' delta_merge(table, source, "target.id = source.id") |>
#'   when_not_matched_by_source_update(c(active = "false"))
#' }
#'
#' @export
when_not_matched_by_source_update <- new_generic(
  "when_not_matched_by_source_update",
  "builder",
  function(builder, ...) {
    S7::S7_dispatch()
  }
)

method(when_not_matched_by_source_update, DeltaMergeBuilder) <- function(
  builder,
  updates,
  predicate = NULL
) {
  if (is.null(updates) || length(updates) == 0) {
    stop("'updates' must be a non-empty named vector")
  }

  if (is.null(names(updates)) || any(names(updates) == "")) {
    stop("'updates' must be a named vector with column names")
  }

  clause <- list(updates = as.list(updates))
  if (!is.null(predicate)) {
    if (!is.character(predicate) || length(predicate) != 1) {
      stop("'predicate' must be a single character string")
    }
    clause$predicate <- predicate
  }

  builder@not_matched_by_source_update_clauses <- c(
    builder@not_matched_by_source_update_clauses,
    list(clause)
  )
  builder
}

#' Add WHEN NOT MATCHED BY SOURCE THEN DELETE clause
#'
#' Deletes target rows that don't have a match in the source data.
#' This is useful for syncing a target table to exactly match the source.
#'
#' @param builder A DeltaMergeBuilder object.
#' @param ... Additional arguments passed to methods.
#'
#' @section Method arguments:
#' The method for DeltaMergeBuilder accepts:
#' \describe{
#'   \item{predicate}{Optional character. Additional predicate to filter which non-matched
#'     target rows should be deleted.}
#' }
#'
#' @return The modified DeltaMergeBuilder (for method chaining).
#'
#' @examples
#' \dontrun{
#' # Delete target rows that are not in source
#' delta_merge(table, source, "target.id = source.id") |>
#'   when_not_matched_by_source_delete()
#'
#' # Delete with a condition
#' delta_merge(table, source, "target.id = source.id") |>
#'   when_not_matched_by_source_delete(predicate = "target.deletable = true")
#' }
#'
#' @export
when_not_matched_by_source_delete <- new_generic(
  "when_not_matched_by_source_delete",
  "builder",
  function(builder, ...) {
    S7::S7_dispatch()
  }
)

method(when_not_matched_by_source_delete, DeltaMergeBuilder) <- function(
  builder,
  predicate = NULL
) {
  clause <- list()
  if (!is.null(predicate)) {
    if (!is.character(predicate) || length(predicate) != 1) {
      stop("'predicate' must be a single character string")
    }
    clause$predicate <- predicate
  }

  builder@not_matched_by_source_delete_clauses <- c(
    builder@not_matched_by_source_delete_clauses,
    list(clause)
  )
  builder
}

# ==============================================================================
# Execute Method
# ==============================================================================

#' Execute the MERGE operation
#'
#' Executes the configured merge operation against the target Delta table.
#'
#' @param builder A DeltaMergeBuilder object configured with merge clauses.
#' @param ... Additional arguments passed to methods.
#'
#' @return A named list with merge metrics:
#'   \itemize{
#'     \item \code{num_target_rows_inserted}: Number of rows inserted into target.
#'     \item \code{num_target_rows_updated}: Number of rows updated in target.
#'     \item \code{num_target_rows_deleted}: Number of rows deleted from target.
#'     \item \code{num_target_files_added}: Number of files added.
#'     \item \code{num_target_files_removed}: Number of files removed.
#'     \item \code{num_target_rows_copied}: Number of rows copied (unchanged).
#'     \item \code{num_output_rows}: Total number of rows in output.
#'     \item \code{execution_time_ms}: Execution time in milliseconds.
#'   }
#'
#' @examples
#' \dontrun{
#' result <- delta_merge("path/to/table", source, "target.id = source.id") |>
#'   when_matched_update(c(value = "source.value")) |>
#'   when_not_matched_insert(c(id = "source.id", value = "source.value")) |>
#'   merge_execute()
#'
#' print(result)
#' # $num_target_rows_updated
#' # [1] 5
#' # $num_target_rows_inserted
#' # [1] 3
#' # ...
#' }
#'
#' @export
merge_execute <- new_generic(
  "merge_execute",
  "builder",
  function(builder, ...) {
    S7::S7_dispatch()
  }
)

method(merge_execute, DeltaMergeBuilder) <- function(builder) {
  # Validate that at least one clause is defined
  total_clauses <- length(builder@matched_update_clauses) +
    length(builder@matched_delete_clauses) +
    length(builder@not_matched_insert_clauses) +
    length(builder@not_matched_by_source_update_clauses) +
    length(builder@not_matched_by_source_delete_clauses)

  if (total_clauses == 0) {
    stop(
      "At least one merge clause must be specified (e.g., when_matched_update, when_not_matched_insert)"
    )
  }

  # Convert source data to Arrow stream
  stream <- nanoarrow::as_nanoarrow_array_stream(builder@source_data)

  # Call single Rust function with all configuration
  result <- delta_merge_execute(
    table_uri = builder@table_path,
    source_stream = stream,
    predicate = builder@predicate,
    source_alias = builder@source_alias,
    target_alias = builder@target_alias,
    matched_update_clauses = builder@matched_update_clauses,
    matched_delete_clauses = builder@matched_delete_clauses,
    not_matched_insert_clauses = builder@not_matched_insert_clauses,
    not_matched_by_source_update_clauses = builder@not_matched_by_source_update_clauses,
    not_matched_by_source_delete_clauses = builder@not_matched_by_source_delete_clauses,
    storage_options = builder@storage_options
  )

  if (methods::is(result, "error")) {
    rlang::abort(result$value)
  }

  result
}
