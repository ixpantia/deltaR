# deltaR <img src="man/figures/logo.png" align="right" height="139" />
<!-- badges: start -->
[![R-CMD-check](https://github.com/ixpantia/deltaR/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/ixpantia/deltaR/actions/workflows/R-CMD-check.yaml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
<!-- badges: end -->

**deltaR** is an R interface to [Delta Lake](https://delta.io/), providing full support for reading and writing Delta tables with ACID transactions, time travel, and schema evolution. Built on the high-performance [delta-rs](https://github.com/delta-io/delta-rs) Rust library, deltaR brings the power of Delta Lake to R with minimal overhead.

## Features

- 📖 **Read Delta tables** - Query Delta tables as Arrow tables or data frames
- ✍️ **Write Delta tables** - Create and append to Delta tables with full ACID guarantees
- ⏰ **Time travel** - Access historical versions of your data
- 🔄 **Schema evolution** - Merge or overwrite schemas as your data evolves
- ☁️ **Cloud storage** - Native support for S3, Google Cloud Storage, and Azure Blob Storage
- 🚀 **High performance** - Powered by Rust with memory-efficient streaming writes
- 📊 **Arrow integration** - Seamless interoperability with the Arrow ecosystem

## Installation

### Prerequisites

deltaR requires the Rust toolchain to compile from source:
- **Rust** >= 1.65.0 ([Install Rust](https://rustup.rs/))
- **Cargo** (included with Rust)

### Install from GitHub

```r
# Install remotes if needed
install.packages("remotes")

# Install deltaR
remotes::install_github("ixpantia/deltaR")
```

## Quick Start

### Writing Data

```r
library(deltaR)

# Create a data frame
df <- data.frame(
  id = 1:1000,
  name = sample(letters, 1000, replace = TRUE),
  value = runif(1000),
  date = as.Date("2024-01-01") + sample(0:365, 1000, replace = TRUE)
)

# Write to a Delta table
write_deltalake(df, "path/to/my_table")

# Append more data
new_data <- data.frame(

id = 1001:1100,
  name = sample(letters, 100, replace = TRUE),
  value = runif(100),
  date = as.Date("2025-01-01") + sample(0:30, 100, replace = TRUE)
)
write_deltalake(new_data, "path/to/my_table", mode = "append")

# Overwrite the table
write_deltalake(df, "path/to/my_table", mode = "overwrite")
```

### Reading Data

```r
# Open a Delta table
dt <- delta_table("path/to/my_table")

# Get table information
dt$version()
dt$schema()
dt$num_files()

# Read as Arrow Table
arrow_table <- dt$to_arrow()

# Read as data.frame
df <- dt$to_data_frame()

# Read with dplyr
library(dplyr)
dt$to_arrow() |>
  filter(value > 0.5) |>
  group_by(name) |>
  summarise(total = sum(value)) |>
  collect()
```
### Time Travel

```r
# Load a specific version
dt$load_version(5)

# Load data as of a specific timestamp
dt$load_datetime("2024-06-15T10:30:00Z")

# View table history
dt$history()
```

### Partitioned Tables

```r
# Create a partitioned table
write_deltalake(
  df,
  "path/to/partitioned_table",
  partition_by = c("date", "name")
)
```

### Cloud Storage

```r
# Google Cloud Storage
write_deltalake(
  df,
  "gs://my-bucket/delta_table",
  storage_options = list(
    google_service_account_path = "path/to/credentials.json"
  )
)

# Amazon S3
write_deltalake(
  df,
  "s3://my-bucket/delta_table",
  storage_options = list(
    aws_access_key_id = Sys.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
    aws_region = "us-east-1"
  )
)

# Azure Blob Storage
write_deltalake(
  df,
  "az://my-container/delta_table",
  storage_options = list(
    azure_storage_account_name = Sys.getenv("AZURE_STORAGE_ACCOUNT"),
    azure_storage_account_key = Sys.getenv("AZURE_STORAGE_KEY")
  )
)
```

## Schema Evolution

deltaR supports schema evolution when appending data:

```r
# Original table
df1 <- data.frame(id = 1:5, name = letters[1:5])
write_deltalake(df1, "path/to/table")

# Append with a new column (requires schema_mode = "merge")
df2 <- data.frame(id = 6:10, name = letters[6:10], score = runif(5))
write_deltalake(df2, "path/to/table", mode = "append", schema_mode = "merge")
```

## Supported Data Types

deltaR supports all Delta Lake compatible Arrow types:

| R Type | Arrow Type | Delta Lake Type |
|--------|------------|-----------------|
| `integer` | Int32 | Integer |
| `double` | Float64 | Double |
| `character` | Utf8 | String |
| `logical` | Boolean | Boolean |
| `Date` | Date32 | Date |
| `POSIXct` | Timestamp | Timestamp |
| `raw` | Binary | Binary |
| `factor` | Dictionary | String |

**Note:** Some Arrow types are not supported by Delta Lake, including Time, Duration, and Interval types. Attempting to write these types will result in an error with a helpful message suggesting alternatives.

## Table Maintenance

### Vacuum

Remove old files no longer referenced by the Delta table:

```r
dt <- delta_table("path/to/table")

# Dry run - see what would be deleted
dt$vacuum(retention_hours = 168, dry_run = TRUE)

# Actually delete old files
dt$vacuum(retention_hours = 168, dry_run = FALSE)
```

## Performance Tips

1. **Use partitioning** for large tables that are frequently filtered by specific columns
2. **Set `target_file_size`** to control output file sizes for better read performance
3. **Use Arrow** for downstream processing instead of converting to data.frame
4. **Vacuum regularly** to remove old files and reduce storage costs

```r
# Control output file size (in bytes)
write_deltalake(
  large_df,
  "path/to/table",
  target_file_size = 128 * 1024 * 1024  
)
```

## Documentation

- [Getting Started Vignette](vignettes/getting-started.Rmd)
- [Cloud Storage Guide](vignettes/cloud-storage.Rmd)
- [Function Reference](reference/index.html)

## Acknowledgments

deltaR is built on the shoulders of giants:

- **[delta-rs](https://github.com/delta-io/delta-rs)** - The Rust implementation of Delta Lake that powers this package. We are grateful to the delta-rs maintainers and contributors for their excellent work.
- **[Delta Lake](https://delta.io/)** - The open-source storage layer that brings reliability to data lakes.
- **[Apache Arrow](https://arrow.apache.org/)** - The columnar memory format that enables high-performance data interchange.
- **[extendr](https://extendr.github.io/)** - The framework that makes it possible to call Rust from R.
- **[nanoarrow](https://github.com/apache/arrow-nanoarrow)** - Lightweight Arrow implementation for R.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Related Projects

- [arrow](https://arrow.apache.org/docs/r/) - R package for Apache Arrow
- [duckdb](https://duckdb.org/docs/api/r) - DuckDB R API (can read Delta tables)
- [sparklyr](https://spark.rstudio.com/) - R interface for Apache Spark (supports Delta Lake)

---

Made with ❤️ by [ixpantia](https://www.ixpantia.com/)
