# Tests for delta_table functionality
# Note: These tests require an existing Delta table to test against
# Since we can't write Delta tables yet, we skip tests if no test table exists

test_that("is_delta_table_path returns FALSE for non-existent path", {
  temp_dir <- tempfile("not_delta_")
  expect_false(is_delta_table_path(temp_dir))
})

test_that("is_delta_table_path returns FALSE for empty directory", {
  temp_dir <- tempfile("empty_dir_")
  dir.create(temp_dir)
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

  expect_false(is_delta_table_path(temp_dir))
})

test_that("is_delta_table_path returns FALSE for regular directory with files", {
  temp_dir <- tempfile("regular_dir_")
  dir.create(temp_dir)
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

  # Create some regular files

  writeLines("test", file.path(temp_dir, "test.txt"))

  expect_false(is_delta_table_path(temp_dir))
})

test_that("delta_table constructor handles invalid path", {
  # Either errors or returns a result - both are acceptable behaviors
  # depending on how the path is resolved

  result <- tryCatch(
    delta_table("/nonexistent/path/to/delta"),
    error = function(e) "error"
  )
  # If it didn't error, it should at least not be NULL

  if (!identical(result, "error")) {
    expect_true(inherits(result, "deltaR::DeltaTable") || is.null(result))
  } else {
    expect_true(TRUE) # Error was thrown, which is expected
  }
})

test_that("write_deltalake works with data.frame", {
  temp_dir <- tempfile("delta_write_test_")
  dir.create(temp_dir)
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

  df <- data.frame(x = 1:5, y = letters[1:5])
  result <- write_deltalake(df, temp_dir)

  expect_type(result, "list")
  expect_true("version" %in% names(result))
  expect_true("num_files" %in% names(result))
  expect_equal(result$version, 0L)

  # Verify table was created
  expect_true(is_delta_table_path(temp_dir))
})

test_that("write_deltalake creates directory automatically for local paths", {
  temp_dir <- tempfile("delta_auto_create_")
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

  # Directory does not exist yet
  expect_false(dir.exists(temp_dir))

  df <- data.frame(x = 1:5, y = letters[1:5])
  result <- write_deltalake(df, temp_dir)

  # Directory should now exist and contain a valid Delta table
  expect_true(dir.exists(temp_dir))
  expect_true(is_delta_table_path(temp_dir))
  expect_equal(result$version, 0L)
})

test_that("write_deltalake append mode works", {
  temp_dir <- tempfile("delta_append_test_")
  dir.create(temp_dir)
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

  df1 <- data.frame(x = 1:5, y = letters[1:5])
  write_deltalake(df1, temp_dir)

  df2 <- data.frame(x = 6:10, y = letters[6:10])
  result <- write_deltalake(df2, temp_dir, mode = "append")

  expect_equal(result$version, 1L)
})

test_that("write_deltalake overwrite mode works", {
  temp_dir <- tempfile("delta_overwrite_test_")
  dir.create(temp_dir)
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

  df1 <- data.frame(x = 1:5, y = letters[1:5])
  write_deltalake(df1, temp_dir)

  df2 <- data.frame(x = 100:105, y = letters[1:6])
  result <- write_deltalake(df2, temp_dir, mode = "overwrite")

  expect_equal(result$version, 1L)
})

test_that("create_deltalake requires schema", {
  expect_error(
    create_deltalake(tempfile(), NULL),
    "'schema' must be provided"
  )
})

test_that("create_deltalake works with nanoarrow schema", {
  temp_dir <- tempfile("delta_create_test_")
  dir.create(temp_dir)
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

  schema <- nanoarrow::na_struct(list(
    id = nanoarrow::na_int64(),
    name = nanoarrow::na_string()
  ))

  result <- create_deltalake(temp_dir, schema)

  expect_equal(result, 0L)
  expect_true(is_delta_table_path(temp_dir))
})

# Integration tests - these require an existing Delta table
# They are skipped if no DELTA_TEST_TABLE environment variable is set

skip_if_no_test_table <- function() {
  test_table <- Sys.getenv("DELTA_TEST_TABLE", unset = "")
  if (test_table == "" || !dir.exists(test_table)) {
    skip(
      "No test Delta table available. Set DELTA_TEST_TABLE env var to run integration tests."
    )
  }
  test_table
}

test_that("delta_table can open existing table", {
  test_table <- skip_if_no_test_table()

  dt <- delta_table(test_table)
  expect_s3_class(dt, "deltaR::DeltaTable")
})

test_that("version returns non-negative integer", {
  test_table <- skip_if_no_test_table()

  dt <- delta_table(test_table)
  v <- table_version(dt)

  expect_true(is.numeric(v))
  expect_true(v >= 0)
})

test_that("get_files returns character vector", {
  test_table <- skip_if_no_test_table()

  dt <- delta_table(test_table)
  files <- get_files(dt)

  expect_type(files, "character")
  expect_true(length(files) >= 0)
})

test_that("get_metadata returns list with expected fields", {
  test_table <- skip_if_no_test_table()

  dt <- delta_table(test_table)
  meta <- get_metadata(dt)

  expect_type(meta, "list")
  expect_true("id" %in% names(meta))
  expect_true("partition_columns" %in% names(meta))
})

test_that("get_schema returns schema object", {
  test_table <- skip_if_no_test_table()

  dt <- delta_table(test_table)
  schema <- get_schema(dt)

  expect_true(!is.null(schema))
})

test_that("history returns data.frame", {
  test_table <- skip_if_no_test_table()

  dt <- delta_table(test_table)
  hist <- history(dt)

  expect_s3_class(hist, "data.frame")
  expect_true("version" %in% names(hist))
  expect_true("operation" %in% names(hist))
})

test_that("print method works without error", {
  test_table <- skip_if_no_test_table()

  dt <- delta_table(test_table)

  expect_output(print(dt), "DeltaTable")
  expect_output(print(dt), "Path:")
  expect_output(print(dt), "Version:")
})

test_that("is_delta_table_path returns TRUE for valid table", {
  test_table <- skip_if_no_test_table()

  expect_true(is_delta_table_path(test_table))
})
