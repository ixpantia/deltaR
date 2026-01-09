# ==============================================================================
# Basic MERGE Tests
# ==============================================================================

test_that("delta_merge performs basic update on matched rows", {
  temp_dir <- tempfile("delta_merge_update_")
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

  # Create target table
  target <- data.frame(x = 1:3, y = c(4L, 5L, 6L))
  write_deltalake(target, temp_dir)

  # Create source data
  source <- data.frame(x = c(2L, 3L), y = c(50L, 80L))

  # Perform merge
  result <- delta_merge(temp_dir, source, "target.x = source.x") |>
    when_matched_update(c(y = "source.y")) |>
    merge_execute()

  expect_equal(result$num_target_rows_updated, 2)
  expect_equal(result$num_target_rows_inserted, 0)
  expect_equal(result$num_target_rows_deleted, 0)

  # Verify the data
  dt <- delta_table(temp_dir)
  files <- get_files(dt)
  updated_data <- arrow::read_parquet(files)
  updated_data <- updated_data[order(updated_data$x), ]
  expect_equal(updated_data$y, c(4L, 50L, 80L))
})

test_that("delta_merge performs insert on non-matched rows", {
  temp_dir <- tempfile("delta_merge_insert_")
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

  # Create target table
  target <- data.frame(x = 1:3, y = c(4L, 5L, 6L))
  write_deltalake(target, temp_dir)

  # Create source with new rows
  source <- data.frame(x = c(4L, 5L), y = c(7L, 8L))

  # Perform merge
  result <- delta_merge(temp_dir, source, "target.x = source.x") |>
    when_not_matched_insert(c(x = "source.x", y = "source.y")) |>
    merge_execute()

  expect_equal(result$num_target_rows_inserted, 2)
  expect_equal(result$num_target_rows_updated, 0)

  # Verify the data
  dt <- delta_table(temp_dir)
  files <- get_files(dt)
  updated_data <- arrow::open_dataset(files) |> dplyr::collect()
  expect_equal(nrow(updated_data), 5)
})

test_that("delta_merge performs upsert (update + insert)", {
  temp_dir <- tempfile("delta_merge_upsert_")
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

  # Create target table
  target <- data.frame(x = 1:3, y = c(4L, 5L, 6L))
  write_deltalake(target, temp_dir)

  # Create source with updates and new rows
  source <- data.frame(x = c(2L, 3L, 5L), y = c(50L, 80L, 110L))

  # Perform upsert
  result <- delta_merge(temp_dir, source, "target.x = source.x") |>
    when_matched_update(c(x = "source.x", y = "source.y")) |>
    when_not_matched_insert(c(x = "source.x", y = "source.y")) |>
    merge_execute()

  expect_equal(result$num_target_rows_updated, 2)
  expect_equal(result$num_target_rows_inserted, 1)

  # Verify the data
  dt <- delta_table(temp_dir)
  files <- get_files(dt)
  updated_data <- arrow::read_parquet(files)
  expect_equal(nrow(updated_data), 4)
})

test_that("delta_merge performs delete on matched rows", {
  temp_dir <- tempfile("delta_merge_delete_")
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

  # Create target table
  target <- data.frame(x = 1:3, y = c(4L, 5L, 6L))
  write_deltalake(target, temp_dir)

  # Create source - rows to delete
  source <- data.frame(x = c(2L, 3L), deleted = c(FALSE, TRUE))

  # Perform conditional delete
  result <- delta_merge(temp_dir, source, "target.x = source.x") |>
    when_matched_delete(predicate = "source.deleted = true") |>
    merge_execute()

  expect_equal(result$num_target_rows_deleted, 1)

  # Verify the data
  dt <- delta_table(temp_dir)
  files <- get_files(dt)
  updated_data <- arrow::read_parquet(files)
  expect_equal(nrow(updated_data), 2)
})

test_that("delta_merge with when_not_matched_by_source_delete removes stale rows", {
  temp_dir <- tempfile("delta_merge_source_delete_")
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

  # Create target table
  target <- data.frame(x = 1:3, y = c(4L, 5L, 6L))
  write_deltalake(target, temp_dir)

  # Source only has some rows - others should be deleted
  source <- data.frame(x = c(2L, 3L), y = c(5L, 6L))

  # Perform merge with delete of unmatched target rows
  result <- delta_merge(temp_dir, source, "target.x = source.x") |>
    when_not_matched_by_source_delete() |>
    merge_execute()

  expect_equal(result$num_target_rows_deleted, 1) # Row with x=1 deleted

  # Verify the data
  dt <- delta_table(temp_dir)
  files <- get_files(dt)
  updated_data <- arrow::read_parquet(files)
  expect_equal(nrow(updated_data), 2)
  expect_false(1L %in% updated_data$x)
})

# ==============================================================================
# UPDATE ALL and INSERT ALL Tests (currently not supported)
# ==============================================================================

test_that("when_matched_update_all warns and errors on execute", {
  temp_dir <- tempfile("delta_merge_update_all_")
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

  # Create target table
  target <- data.frame(x = 1:3, y = c(4L, 5L, 6L))
  write_deltalake(target, temp_dir)

  # Create source with matching columns
  source <- data.frame(x = c(2L, 3L), y = c(50L, 80L))

  # when_matched_update_all should warn and error on execute
  expect_warning(
    builder <- delta_merge(temp_dir, source, "target.x = source.x") |>
      when_matched_update_all(),
    "not yet fully supported"
  )

  expect_error(merge_execute(builder))
})

test_that("when_not_matched_insert_all warns and errors on execute", {
  temp_dir <- tempfile("delta_merge_insert_all_")
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

  # Create target table
  target <- data.frame(x = 1:3, y = c(4L, 5L, 6L))
  write_deltalake(target, temp_dir)

  # Create source with new rows
  source <- data.frame(x = c(4L, 5L), y = c(7L, 8L))

  # when_not_matched_insert_all should warn and error on execute
  expect_warning(
    builder <- delta_merge(temp_dir, source, "target.x = source.x") |>
      when_not_matched_insert_all(),
    "not yet fully supported"
  )

  expect_error(merge_execute(builder))
})

# ==============================================================================
# Builder Pattern Tests
# ==============================================================================

test_that("DeltaMergeBuilder accumulates clauses correctly", {
  temp_dir <- tempfile("delta_merge_builder_")

  builder <- delta_merge(temp_dir, data.frame(x = 1), "target.x = source.x") |>
    when_matched_update(c(y = "source.y")) |>
    when_matched_update(c(z = "source.z"), predicate = "source.flag = true") |>
    when_not_matched_insert(c(x = "source.x", y = "source.y"))

  expect_length(builder@matched_update_clauses, 2)
  expect_length(builder@not_matched_insert_clauses, 1)
})

test_that("delta_merge accepts DeltaTable object", {
  temp_dir <- tempfile("delta_merge_dt_obj_")
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

  target <- data.frame(x = 1:3, y = c(4L, 5L, 6L))
  write_deltalake(target, temp_dir)

  dt <- delta_table(temp_dir)
  source <- data.frame(x = c(2L), y = c(99L))

  result <- delta_merge(dt, source, "target.x = source.x") |>
    when_matched_update(c(y = "source.y")) |>
    merge_execute()

  expect_equal(result$num_target_rows_updated, 1)
})

test_that("DeltaMergeBuilder print method works", {
  temp_dir <- tempfile("delta_merge_print_")

  builder <- delta_merge(temp_dir, data.frame(x = 1), "target.x = source.x") |>
    when_matched_update(c(y = "source.y"))

  expect_output(print(builder), "DeltaMergeBuilder")
  expect_output(print(builder), "Predicate:")
  expect_output(print(builder), "matched_update:")
})

# ==============================================================================
# Edge Cases
# ==============================================================================

test_that("delta_merge handles empty source data", {
  temp_dir <- tempfile("delta_merge_empty_")
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

  target <- data.frame(x = 1:3, y = c(4L, 5L, 6L))
  write_deltalake(target, temp_dir)

  source <- data.frame(x = integer(0), y = integer(0))

  result <- delta_merge(temp_dir, source, "target.x = source.x") |>
    when_matched_update(c(y = "source.y")) |>
    merge_execute()

  expect_equal(result$num_target_rows_updated, 0)
})

test_that("delta_merge handles no matching rows", {
  temp_dir <- tempfile("delta_merge_no_match_")
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

  target <- data.frame(x = 1:3, y = c(4L, 5L, 6L))
  write_deltalake(target, temp_dir)

  source <- data.frame(x = c(100L, 200L), y = c(1L, 2L))

  result <- delta_merge(temp_dir, source, "target.x = source.x") |>
    when_matched_update(c(y = "source.y")) |>
    when_not_matched_insert(c(x = "source.x", y = "source.y")) |>
    merge_execute()

  expect_equal(result$num_target_rows_updated, 0)
  expect_equal(result$num_target_rows_inserted, 2)
})

test_that("delta_merge handles conditional updates", {
  temp_dir <- tempfile("delta_merge_conditional_")
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

  # Create target table
  target <- data.frame(x = 1:3, y = c(4L, 5L, 6L))
  write_deltalake(target, temp_dir)

  # Create source with conditional flag
  source <- data.frame(x = c(1L, 2L, 3L), y = c(10L, 20L, 30L))

  # Only update where source.y > 15
  result <- delta_merge(temp_dir, source, "target.x = source.x") |>
    when_matched_update(c(y = "source.y"), predicate = "source.y > 15") |>
    merge_execute()

  expect_equal(result$num_target_rows_updated, 2) # Only rows 2 and 3 updated
})

# ==============================================================================
# Input Validation Tests
# ==============================================================================

test_that("delta_merge errors on empty predicate", {
  expect_error(
    delta_merge("path/to/table", data.frame(x = 1), ""),
    "'predicate' must be a non-empty character string"
  )
})

test_that("delta_merge errors on invalid table input", {
  expect_error(
    delta_merge(123, data.frame(x = 1), "target.x = source.x"),
    "'table' must be a DeltaTable object or a single character path"
  )
})

test_that("when_matched_update errors on empty updates", {
  builder <- delta_merge(
    "path/to/table",
    data.frame(x = 1),
    "target.x = source.x"
  )

  expect_error(
    when_matched_update(builder, c()),
    "'updates' must be a non-empty named vector"
  )
})

test_that("when_matched_update errors on unnamed updates", {
  builder <- delta_merge(
    "path/to/table",
    data.frame(x = 1),
    "target.x = source.x"
  )

  expect_error(
    when_matched_update(builder, c("source.y")),
    "'updates' must be a named vector with column names"
  )
})

test_that("merge_execute errors when no clauses defined", {
  temp_dir <- tempfile("delta_merge_no_clause_")
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

  target <- data.frame(x = 1:3, y = c(4L, 5L, 6L))
  write_deltalake(target, temp_dir)

  source <- data.frame(x = c(2L), y = c(50L))

  expect_error(
    delta_merge(temp_dir, source, "target.x = source.x") |>
      merge_execute(),
    "At least one merge clause must be specified"
  )
})

# ==============================================================================
# Error Handling Tests
# ==============================================================================

test_that("delta_merge errors on invalid predicate", {
  temp_dir <- tempfile("delta_merge_bad_pred_")
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

  target <- data.frame(x = 1:3, y = c(4L, 5L, 6L))
  write_deltalake(target, temp_dir)

  source <- data.frame(x = c(2L), y = c(50L))

  expect_error(
    delta_merge(temp_dir, source, "invalid syntax !!!") |>
      when_matched_update(c(y = "source.y")) |>
      merge_execute()
  )
})

test_that("delta_merge handles updates to nonexistent columns", {
  # Note: delta-rs allows updating columns that don't exist in target
  # This creates a new column in the output
  temp_dir <- tempfile("delta_merge_bad_col_")

  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

  target <- data.frame(x = 1:3, y = c(4L, 5L, 6L))
  write_deltalake(target, temp_dir)

  source <- data.frame(x = c(2L), y = c(50L))

  # This should succeed - delta-rs will add the new column
  result <- delta_merge(temp_dir, source, "target.x = source.x") |>
    when_matched_update(c(nonexistent_col = "source.y")) |>
    merge_execute()

  expect_equal(result$num_target_rows_updated, 1)
})

# ==============================================================================
# Metrics Tests
# ==============================================================================

test_that("merge_execute returns all expected metrics", {
  temp_dir <- tempfile("delta_merge_metrics_")
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

  target <- data.frame(x = 1:3, y = c(4L, 5L, 6L))
  write_deltalake(target, temp_dir)

  source <- data.frame(x = c(2L, 4L), y = c(50L, 70L))

  result <- delta_merge(temp_dir, source, "target.x = source.x") |>
    when_matched_update(c(y = "source.y")) |>
    when_not_matched_insert(c(x = "source.x", y = "source.y")) |>
    merge_execute()

  # Check that all expected metric fields are present
  expect_true("num_target_rows_inserted" %in% names(result))
  expect_true("num_target_rows_updated" %in% names(result))
  expect_true("num_target_rows_deleted" %in% names(result))
  expect_true("num_target_files_added" %in% names(result))
  expect_true("num_target_files_removed" %in% names(result))
  expect_true("num_target_rows_copied" %in% names(result))
  expect_true("num_output_rows" %in% names(result))
  expect_true("execution_time_ms" %in% names(result))

  # Verify specific metrics
  expect_equal(result$num_target_rows_updated, 1)
  expect_equal(result$num_target_rows_inserted, 1)
})
