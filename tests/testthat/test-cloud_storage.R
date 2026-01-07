# Tests for cloud storage functionality

test_that("is_local_path identifies cloud URIs correctly", {
  # Internal function check
  expect_false(deltaR:::is_local_path("gs://bucket/path"))
  expect_false(deltaR:::is_local_path("s3://bucket/path"))
  expect_false(deltaR:::is_local_path("az://bucket/path"))
  expect_false(deltaR:::is_local_path("abfs://bucket/path"))
  expect_true(deltaR:::is_local_path("/local/path"))
  expect_true(deltaR:::is_local_path("C:/local/path"))
  expect_true(deltaR:::is_local_path("./relative/path"))
})

test_that("is_delta_table_path handles cloud URIs without crashing", {
  # This should return FALSE but not crash, as it will fail to connect/find the table
  # but the URL parsing and storage options plumbing should work.
  expect_false(is_delta_table_path(
    "gs://nonexistent-bucket-deltaR-test/table",
    storage_options = list(google_service_account_path = "nonexistent.json")
  ))
})

# Integration tests for GCS
# These require valid credentials and a writable bucket.
# To run these, set the following environment variables:
# Sys.setenv(DELTA_GCS_TEST_URI = "gs://your-bucket/path/to/table")
# Sys.setenv(GOOGLE_SERVICE_ACCOUNT_PATH = "/path/to/your/key.json")

skip_if_no_gcs <- function() {
  uri <- Sys.getenv("DELTA_GCS_TEST_URI", unset = "")
  if (uri == "") {
    skip("GCS integration tests skipped. Set DELTA_GCS_TEST_URI to run.")
  }
  uri
}

test_that("GCS write and read works (Integration)", {
  uri <- skip_if_no_gcs()

  key_path <- Sys.getenv("GOOGLE_SERVICE_ACCOUNT_PATH", unset = "")
  opts <- if (key_path != "") {
    list(google_service_account_path = key_path)
  } else {
    list()
  }

  df <- data.frame(
    id = 1:10,
    val = rnorm(10),
    grp = rep(c("A", "B"), each = 5)
  )

  # Test write
  res <- write_deltalake(
    df,
    uri,
    mode = "overwrite",
    storage_options = opts,
    partition_by = "grp"
  )

  expect_type(res, "list")
  expect_true(res$version >= 0)

  # Test opening the table
  dt <- delta_table(uri, storage_options = opts)
  expect_s3_class(dt, "deltaR::DeltaTable")
  expect_equal(table_version(dt), res$version)

  # Verify files are in GCS
  files <- get_files(dt)
  expect_type(files, "character")
  expect_true(length(files) > 0)
  expect_true(all(grepl("^gs://", files)))
})
