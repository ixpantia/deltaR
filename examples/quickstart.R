# ==============================================================================
# deltaR Quick Start Guide
# ==============================================================================
# A minimal example showing the most common deltaR operations
# ==============================================================================

library(deltaR)

# ------------------------------------------------------------------------------
# 1. Write a data.frame to a new Delta table
# ------------------------------------------------------------------------------

# Create sample data
df <- data.frame(
  id = 1:5,
  name = c("Alice", "Bob", "Charlie", "Diana", "Eve"),
  score = c(85.5, 92.0, 78.5, 95.0, 88.5)
)

# Write to a new Delta table (directory is created automatically)
table_path <- tempfile("my_delta_table_")
write_deltalake(df, table_path)

# ------------------------------------------------------------------------------
# 2. Open and explore the table
# ------------------------------------------------------------------------------

# Open the table
dt <- delta_table(table_path)

# Basic info
print(dt)
table_version(dt)
get_schema(dt)
get_files(dt)

# ------------------------------------------------------------------------------
# 3. Append more data
# ------------------------------------------------------------------------------

new_data <- data.frame(
  id = 6:8,
  name = c("Frank", "Grace", "Henry"),
  score = c(91.0, 87.5, 82.0)
)

write_deltalake(new_data, table_path, mode = "append")

# ------------------------------------------------------------------------------
# 4. Overwrite all data
# ------------------------------------------------------------------------------

replacement_data <- data.frame(
  id = 1:3,
  name = c("Updated1", "Updated2", "Updated3"),
  score = c(100, 100, 100)
)

write_deltalake(replacement_data, table_path, mode = "overwrite")

# ------------------------------------------------------------------------------
# 5. Time travel - access previous versions
# ------------------------------------------------------------------------------

# View history
dt <- delta_table(table_path)
history(dt)

# Load a specific version
dt_v0 <- delta_table(table_path, version = 0)
table_version(dt_v0) # Returns 0

# ------------------------------------------------------------------------------
# 6. Create an empty table with a schema
# ------------------------------------------------------------------------------

schema <- nanoarrow::na_struct(list(
  user_id = nanoarrow::na_int64(),
  email = nanoarrow::na_string(),
  active = nanoarrow::na_bool()
))

empty_table_path <- tempfile("empty_table_")
create_deltalake(empty_table_path, schema, name = "users")

# ------------------------------------------------------------------------------
# 7. Create a partitioned table
# ------------------------------------------------------------------------------

events <- data.frame(
  event_id = 1:10,
  category = rep(c("A", "B"), 5),
  value = runif(10)
)

partitioned_path <- tempfile("partitioned_")
write_deltalake(events, partitioned_path, partition_by = "category")

# Check partition columns
dt_part <- delta_table(partitioned_path)
partition_columns(dt_part) # Returns "category"

# ------------------------------------------------------------------------------
# 8. Check if a path is a Delta table
# ------------------------------------------------------------------------------

is_delta_table_path(table_path) # TRUE
is_delta_table_path("/tmp") # FALSE

# ------------------------------------------------------------------------------
# Cleanup
# ------------------------------------------------------------------------------

unlink(table_path, recursive = TRUE)
unlink(empty_table_path, recursive = TRUE)
unlink(partitioned_path, recursive = TRUE)
