#' @import S7
NULL

.onLoad <- function(...) {
  # Register cloud storage handlers (GCS, S3, Azure) for Delta Lake

  register_cloud_handlers()

  S7::methods_register()
}

#' DeltaTable S7 Class
#'
#' An S7 class representing a Delta Lake table.
#'
#' @param path Character. The path to the Delta table (local or cloud storage URI).
#' @param internal The internal Rust DeltaTableInternal object.
#'
#' @export
DeltaTable <- new_class(
  "DeltaTable",
  properties = list(
    path = new_property(class_character, default = ""),
    internal = new_property(class_any, default = NULL)
  ),
  validator = function(self) {
    if (is.null(self@internal)) {
      return("DeltaTable must have an internal object")
    }
    NULL
  }
)

# Print method for DeltaTable (S7 method registration)
method(print, DeltaTable) <- function(x, ...) {
  cat("DeltaTable\n")
  cat("  Path:", x@path, "\n")
  if (!is.null(x@internal)) {
    cat("  Version:", x@internal$version(), "\n")
    cat("  Files:", x@internal$num_files(), "\n")
  }
  invisible(x)
}
