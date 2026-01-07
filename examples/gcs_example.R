#' Google Cloud Storage (GCS) Example for deltaR
#'
#' This script demonstrates how to interact with Delta tables stored in GCS.
#' To run this example, you need:
#' 1. A Google Cloud Project with GCS enabled.
#' 2. A Service Account key (JSON) with permissions to read/write to a bucket.
#' 3. The following environment variables set:

# Sys.setenv(GCS_DELTA_BUCKET = "gs://your-bucket-name/delta-table-test")
# Sys.setenv(GOOGLE_APPLICATION_CREDENTIALS = "/path/to/your/service-account-key.json")

library(deltaR)
library(nanoarrow)

# 1. Setup Configuration from Environment Variables
gcs_uri <- Sys.getenv("GCS_DELTA_BUCKET")
if (gcs_uri == "") {
  message(
    "INFO: GCS_DELTA_BUCKET not set. Using a placeholder for demonstration."
  )
  gcs_uri <- "gs://my-bucket/path/to/delta_table"
}

# Storage options for GCS.
# deltaR uses the object_store crate internally.
# Common options include:
# - google_service_account_path: Path to the JSON key file.
# - google_application_credentials: Same as above.
# - token: A bearer token for authentication.
storage_opts <- list(
  google_service_account_path = Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS")
)

# 2. Define a Schema and Create an Empty Table
# We use nanoarrow to define the schema which is then passed to the Rust layer.
message("Step 2: Creating empty Delta table in GCS...")
schema <- na_struct(list(
  id = na_int64(),
  timestamp = na_timestamp("us", "UTC"),
  value = na_double(),
  category = na_string()
))

# Note: This will only work if you have valid credentials and bucket access.
tryCatch(
  {
    create_deltalake(
      table_uri = gcs_uri,
      schema = schema,
      partition_by = "category",
      storage_options = storage_opts
    )
    message("Table created successfully.")
  },
  error = function(e) {
    message("Table creation failed (expected if no credentials): ", e$message)
  }
)

# 3. Write Data to GCS
message("\nStep 3: Writing data to GCS...")
df <- data.frame(
  id = 1:5,
  timestamp = Sys.time() + 1:5,
  value = runif(5),
  category = c("A", "A", "B", "B", "C")
)

tryCatch(
  {
    res <- write_deltalake(
      data = df,
      table_or_uri = gcs_uri,
      mode = "append",
      storage_options = storage_opts
    )
    message("Write successful. New version: ", res$version)
  },
  error = function(e) {
    message("Write failed: ", e$message)
  }
)

# 4. Open the Table and Inspect Metadata
message("\nStep 4: Opening Delta table and inspecting metadata...")
tryCatch(
  {
    dt <- delta_table(gcs_uri, storage_options = storage_opts)

    print(dt)

    message("Table Version: ", table_version(dt))
    message(
      "Partition Columns: ",
      paste(partition_columns(dt), collapse = ", ")
    )

    # 5. List Parquet Files
    # These URIs are absolute URIs (gs://...) and can be passed to arrow, polars, or duckdb.
    files <- get_files(dt)
    message("Number of Parquet files in current snapshot: ", length(files))
    if (length(files) > 0) {
      print(head(files))
    }

    # 6. Time Travel
    # Let's perform another write to create a new version
    df_new <- data.frame(
      id = 6,
      timestamp = Sys.time(),
      value = 0.5,
      category = "A"
    )
    write_deltalake(
      df_new,
      gcs_uri,
      mode = "append",
      storage_options = storage_opts
    )

    message("Current Version: ", table_version(dt))

    # Load the previous version (0)
    load_version(dt, version = 0)
    message("Traveled back to Version: ", table_version(dt))
  },
  error = function(e) {
    message("Operations failed: ", e$message)
  }
)

message("\nExample script finished.")
