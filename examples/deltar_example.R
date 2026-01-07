# ==============================================================================
# deltaR Package Examples
# ==============================================================================
# This script demonstrates the main functionality of the deltaR package,
# an R interface to Delta Lake via the delta-rs Rust library.
#
# Features demonstrated:
#   - Writing data to Delta tables
#   - Reading Delta tables
#   - Appending and overwriting data
#   - Creating empty tables with schemas
#   - Time travel (loading specific versions)
#   - Table metadata and history
#   - Partitioned tables
# ==============================================================================

library(deltaR)
library(nanoarrow)

# ==============================================================================
# Example 1: Writing a data.frame to a new Delta table
# ==============================================================================

cat("=== Example 1: Writing a data.frame to a new Delta table ===\n")

# Create a temporary directory for our examples
base_dir <- tempfile("deltaR_examples_")
dir.create(base_dir)

# Create sample data
sales_data <- data.frame(
  id = 1:10,
  product = c(
    "Apple",
    "Banana",
    "Cherry",
    "Date",
    "Elderberry",
    "Fig",
    "Grape",
    "Honeydew",
    "Kiwi",
    "Lemon"
  ),
  quantity = c(100, 150, 80, 200, 50, 120, 300, 90, 110, 75),
  price = c(1.50, 0.75, 3.00, 2.50, 4.00, 2.00, 1.25, 3.50, 2.75, 1.00),
  in_stock = c(TRUE, TRUE, FALSE, TRUE, FALSE, TRUE, TRUE, TRUE, FALSE, TRUE)
)

# Write to a new Delta table (directory is created automatically)
table_path <- file.path(base_dir, "sales_table")
result <- write_deltalake(sales_data, table_path)

cat("Table created successfully!\n")
cat("  Version:", result$version, "\n")
cat("  Number of files:", result$num_files, "\n\n")

# ==============================================================================
# Example 2: Opening and exploring a Delta table
# ==============================================================================

cat("=== Example 2: Opening and exploring a Delta table ===\n")

# Open the table
dt <- delta_table(table_path)
print(dt)

# Get table version
cat("\nCurrent version:", table_version(dt), "\n")

# Get the list of Parquet files
files <- get_files(dt)
cat("Parquet files:\n")
for (f in files) {
  cat("  -", basename(f), "\n")
}

# Get table schema
cat("\nTable schema:\n")
schema <- get_schema(dt)
print(schema)

# Get table metadata
cat("\nTable metadata:\n")
meta <- get_metadata(dt)
print(meta)

cat("\n")

# ==============================================================================
# Example 3: Appending data to an existing table
# ==============================================================================

cat("=== Example 3: Appending data to an existing table ===\n")

# Create new data to append
new_sales <- data.frame(
  id = 11:15,
  product = c("Mango", "Nectarine", "Orange", "Papaya", "Quince"),
  quantity = c(60, 140, 250, 45, 30),
  price = c(2.25, 1.75, 0.80, 3.25, 4.50),
  in_stock = c(TRUE, TRUE, TRUE, FALSE, FALSE)
)

# Append to the existing table
result <- write_deltalake(new_sales, table_path, mode = "append")
cat("Data appended successfully!\n")
cat("  New version:", result$version, "\n")
cat("  Number of files:", result$num_files, "\n")

# Reload the table to see changes
dt <- delta_table(table_path)
cat("  Total files now:", length(get_files(dt)), "\n\n")

# ==============================================================================
# Example 4: Overwriting data in a table
# ==============================================================================

cat("=== Example 4: Overwriting data in a table ===\n")

# Create replacement data
updated_sales <- data.frame(
  id = 1:5,
  product = c("Apple", "Banana", "Cherry", "Date", "Elderberry"),
  quantity = c(500, 600, 400, 700, 300),
  price = c(1.25, 0.65, 2.75, 2.25, 3.75),
  in_stock = c(TRUE, TRUE, TRUE, TRUE, TRUE)
)

# Overwrite the table
result <- write_deltalake(updated_sales, table_path, mode = "overwrite")
cat("Table overwritten successfully!\n")
cat("  New version:", result$version, "\n")
cat("  Number of files:", result$num_files, "\n\n")

# ==============================================================================
# Example 5: Time travel - loading previous versions
# ==============================================================================

cat("=== Example 5: Time travel - loading previous versions ===\n")

# Check history
dt <- delta_table(table_path)
hist <- history(dt)
cat("Table history:\n")
print(hist[, c("version", "timestamp", "operation")])

# Load a previous version (version 0 - the original write)
dt_v0 <- delta_table(table_path, version = 0)
cat("\nVersion 0 had", length(get_files(dt_v0)), "file(s)\n")

# Load version 1 (after append)
dt_v1 <- delta_table(table_path, version = 1)
cat("Version 1 had", length(get_files(dt_v1)), "file(s)\n")

# Current version (after overwrite)
dt_current <- delta_table(table_path)
cat("Current version has", length(get_files(dt_current)), "file(s)\n\n")

# ==============================================================================
# Example 6: Creating an empty table with a schema
# ==============================================================================

cat("=== Example 6: Creating an empty table with a schema ===\n")

# Define a schema using nanoarrow
customer_schema <- na_struct(list(
  customer_id = na_int64(),
  name = na_string(),
  email = na_string(),
  signup_date = na_string(),
  total_purchases = na_double(),
  is_active = na_bool()
))

# Create the empty table (directory is created automatically)
customer_table_path <- file.path(base_dir, "customers_table")
result <- create_deltalake(
  customer_table_path,
  customer_schema,
  name = "customers",
  description = "Customer information table"
)

cat("Empty table created at version:", result, "\n")

# Verify it's a valid Delta table
cat("Is valid Delta table:", is_delta_table_path(customer_table_path), "\n")

# Open and inspect
dt_customers <- delta_table(customer_table_path)
cat("Schema of empty table:\n")
print(get_schema(dt_customers))
cat("\n")

# ==============================================================================
# Example 7: Creating a partitioned table
# ==============================================================================

cat("=== Example 7: Creating a partitioned table ===\n")

# Create data with a partition column
events_data <- data.frame(
  event_id = 1:20,
  event_type = rep(c("click", "view", "purchase", "signup"), 5),
  user_id = sample(1000:9999, 20),
  timestamp = Sys.time() + seq(0, 19 * 3600, by = 3600),
  value = round(runif(20, 1, 100), 2)
)

# Write with partitioning (directory is created automatically)
partitioned_path <- file.path(base_dir, "events_partitioned")
result <- write_deltalake(
  events_data,
  partitioned_path,
  partition_by = "event_type"
)

cat("Partitioned table created!\n")
cat("  Version:", result$version, "\n")

# Check partition columns
dt_events <- delta_table(partitioned_path)
cat(
  "  Partition columns:",
  paste(partition_columns(dt_events), collapse = ", "),
  "\n"
)
cat("  Number of files:", length(get_files(dt_events)), "\n\n")

# ==============================================================================
# Example 8: Working with the table in other tools
# ==============================================================================

cat("=== Example 8: Integration with other R tools ===\n")

# Get the Parquet file paths - these can be used with arrow, polars, or duckdb
dt <- delta_table(table_path)
parquet_files <- get_files(dt)

cat("Parquet files can be read by:\n")
cat("  - arrow::open_dataset()\n")
cat("  - polars\n")
cat("  - duckdb\n")
cat("\nExample with arrow (if installed):\n")
cat('  library(arrow)\n')
cat('  ds <- open_dataset(get_files(dt))\n')
cat('  ds |> collect()\n\n')

# ==============================================================================
# Example 9: Vacuum - cleaning up old files
# ==============================================================================

cat("=== Example 9: Vacuum (dry run) ===\n")

# Perform a dry run of vacuum to see what files would be removed
# Note: Files must be older than retention period (default 7 days)
dt <- delta_table(table_path)
files_to_remove <- vacuum(dt, dry_run = TRUE)
cat("Files that would be removed (dry run):", length(files_to_remove), "\n")
cat("(Files are typically kept for 7 days before vacuum removes them)\n\n")

# ==============================================================================
# Example 10: Checking if a path is a Delta table
# ==============================================================================
cat(
  "=== Example 10: Checking paths ===
"
)

cat(
  "Is '",
  table_path,
  "' a Delta table? ",
  is_delta_table_path(table_path),
  "\n",
  sep = ""
)
cat(
  "Is '",
  base_dir,
  "' a Delta table? ",
  is_delta_table_path(base_dir),
  "\n",
  sep = ""
)
cat("Is '/tmp' a Delta table? ", is_delta_table_path("/tmp"), "\n\n", sep = "")

# ==============================================================================
# Cleanup
# ==============================================================================

cat("=== Cleanup ===\n")
cat("Example tables were created in:", base_dir, "\n")
cat("To clean up, run: unlink('", base_dir, "', recursive = TRUE)\n", sep = "")

# Uncomment the following line to automatically clean up:
# unlink(base_dir, recursive = TRUE)

cat("\n=== All examples completed successfully! ===\n")
