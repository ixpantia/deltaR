# Using deltaR with Cloud Storage

## Introduction

deltaR provides native support for reading and writing Delta tables to
cloud storage services, including: - **Amazon S3** - **Google Cloud
Storage (GCS)** - **Azure Blob Storage / Azure Data Lake Storage
(ADLS)**

This guide covers how to configure and use each cloud storage provider
with deltaR.

## General Concepts

### Storage Options

All cloud storage operations in deltaR use the `storage_options`
parameter to pass authentication credentials and configuration. This is
a named list where keys and values depend on the cloud provider.

``` r
library(deltaR)
```

### URI Formats

Each cloud provider uses a specific URI format:

| Provider             | URI Format                                           |
|----------------------|------------------------------------------------------|
| Amazon S3            | `s3://bucket-name/path/to/table`                     |
| Google Cloud Storage | `gs://bucket-name/path/to/table`                     |
| Azure Blob Storage   | `az://container-name/path/to/table`                  |
| Azure Data Lake Gen2 | `abfs://container@account.dfs.core.windows.net/path` |

## Amazon S3

### Authentication Options

deltaR supports multiple authentication methods for S3:

#### 1. Access Keys

``` r
# Using access keys directly
storage_options <- list(
  aws_access_key_id = "YOUR_ACCESS_KEY_ID",
  aws_secret_access_key = "YOUR_SECRET_ACCESS_KEY",
  aws_region = "us-east-1"
)

# Read from S3
dt <- delta_table("s3://my-bucket/delta_table", storage_options = storage_options)

# Write to S3
write_deltalake(
  df,
  "s3://my-bucket/delta_table",
  storage_options = storage_options
)
```

#### 2. Environment Variables (Recommended)

Using environment variables is more secure than hardcoding credentials:

``` r
# Set environment variables (in .Renviron or your shell)
# AWS_ACCESS_KEY_ID=your_access_key
# AWS_SECRET_ACCESS_KEY=your_secret_key
# AWS_REGION=us-east-1

storage_options <- list(
  aws_access_key_id = Sys.getenv("AWS_ACCESS_KEY_ID"),
  aws_secret_access_key = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
  aws_region = Sys.getenv("AWS_REGION", "us-east-1")
)

dt <- delta_table("s3://my-bucket/delta_table", storage_options = storage_options)
```

#### 3. IAM Role (EC2/ECS/Lambda)

When running on AWS infrastructure with an IAM role attached, you
typically only need to specify the region:

``` r
storage_options <- list(
  aws_region = "us-east-1"
)

dt <- delta_table("s3://my-bucket/delta_table", storage_options = storage_options)
```

#### 4. Session Tokens (Temporary Credentials)

For temporary credentials from STS:

``` r
storage_options <- list(
  aws_access_key_id = "YOUR_TEMP_ACCESS_KEY",
  aws_secret_access_key = "YOUR_TEMP_SECRET_KEY",
  aws_session_token = "YOUR_SESSION_TOKEN",
  aws_region = "us-east-1"
)
```

### S3-Compatible Storage

deltaR works with S3-compatible storage services like MinIO, LocalStack,
or Ceph:

``` r
# MinIO example
storage_options <- list(
  aws_access_key_id = "minioadmin",
  aws_secret_access_key = "minioadmin",
  aws_endpoint_url = "http://localhost:9000",
  aws_region = "us-east-1",
  aws_allow_http = "true"
)

dt <- delta_table("s3://my-bucket/table", storage_options = storage_options)
```

### S3 Storage Options Reference

| Option                  | Description                                    |
|-------------------------|------------------------------------------------|
| `aws_access_key_id`     | AWS access key ID                              |
| `aws_secret_access_key` | AWS secret access key                          |
| `aws_session_token`     | Session token for temporary credentials        |
| `aws_region`            | AWS region (e.g., “us-east-1”)                 |
| `aws_endpoint_url`      | Custom endpoint URL for S3-compatible services |
| `aws_allow_http`        | Allow HTTP connections (default: false)        |

## Google Cloud Storage

### Authentication Options

#### 1. Service Account Key File

``` r
storage_options <- list(
  google_service_account_path = "/path/to/service-account-key.json"
)

dt <- delta_table("gs://my-bucket/delta_table", storage_options = storage_options)

write_deltalake(
  df,
  "gs://my-bucket/delta_table",
  storage_options = storage_options
)
```

#### 2. Service Account Key as JSON String

``` r
# Read the key file content
key_json <- readLines("/path/to/service-account-key.json", warn = FALSE)
key_json <- paste(key_json, collapse = "")

storage_options <- list(
  google_service_account_key = key_json
)

dt <- delta_table("gs://my-bucket/delta_table", storage_options = storage_options)
```

#### 3. Application Default Credentials

When running on GCP infrastructure (Compute Engine, Cloud Run, GKE) or
with `gcloud` configured locally:

``` r
# Uses Application Default Credentials automatically
# Make sure you've run: gcloud auth application-default login

dt <- delta_table("gs://my-bucket/delta_table")
```

### GCS Storage Options Reference

| Option                        | Description                           |
|-------------------------------|---------------------------------------|
| `google_service_account_path` | Path to service account JSON key file |
| `google_service_account_key`  | Service account key as JSON string    |

## Azure Blob Storage / ADLS Gen2

### Authentication Options

#### 1. Storage Account Key

``` r
storage_options <- list(
  azure_storage_account_name = "mystorageaccount",
  azure_storage_account_key = "YOUR_ACCOUNT_KEY"
)

dt <- delta_table("az://mycontainer/delta_table", storage_options = storage_options)

write_deltalake(
  df,
  "az://mycontainer/delta_table",
  storage_options = storage_options
)
```

#### 2. Shared Access Signature (SAS) Token

``` r
storage_options <- list(
  azure_storage_account_name = "mystorageaccount",
  azure_storage_sas_token = "?sv=2021-06-08&ss=b&srt=co..."
)

dt <- delta_table("az://mycontainer/delta_table", storage_options = storage_options)
```

#### 3. Service Principal (Client Credentials)

``` r
storage_options <- list(
  azure_storage_account_name = "mystorageaccount",
  azure_storage_client_id = "YOUR_CLIENT_ID",
  azure_storage_client_secret = "YOUR_CLIENT_SECRET",
  azure_storage_tenant_id = "YOUR_TENANT_ID"
)

dt <- delta_table("az://mycontainer/delta_table", storage_options = storage_options)
```

#### 4. Azure Data Lake Storage Gen2

For ADLS Gen2, you can use the `abfs://` or `abfss://` URI schemes:

``` r
storage_options <- list(
  azure_storage_account_name = "mystorageaccount",
  azure_storage_account_key = "YOUR_ACCOUNT_KEY"
)

# abfss:// for secure (TLS) connections
dt <- delta_table(
  "abfss://mycontainer@mystorageaccount.dfs.core.windows.net/delta_table",
  storage_options = storage_options
)
```

### Azure Storage Options Reference

| Option                        | Description                     |
|-------------------------------|---------------------------------|
| `azure_storage_account_name`  | Storage account name            |
| `azure_storage_account_key`   | Storage account access key      |
| `azure_storage_sas_token`     | Shared access signature token   |
| `azure_storage_client_id`     | Service principal client ID     |
| `azure_storage_client_secret` | Service principal client secret |
| `azure_storage_tenant_id`     | Azure AD tenant ID              |

## Complete Examples

### Example 1: ETL Pipeline with S3

``` r
library(deltaR)
library(dplyr)

# Configure S3 storage
s3_options <- list(
  aws_access_key_id = Sys.getenv("AWS_ACCESS_KEY_ID"),
  aws_secret_access_key = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
  aws_region = "us-east-1"
)

# Read source data
source_dt <- delta_table(
  "s3://data-lake/bronze/raw_events",
  storage_options = s3_options
)

# Transform data
transformed_data <- source_dt$to_arrow() |>
  filter(event_type == "purchase") |>
  mutate(
    event_date = as.Date(event_timestamp),
    revenue = quantity * unit_price
  ) |>
  select(event_id, user_id, event_date, product_id, revenue) |>
  collect()

# Write to silver layer
write_deltalake(
  transformed_data,
  "s3://data-lake/silver/purchases",
  mode = "append",
  partition_by = "event_date",
  storage_options = s3_options
)
```

### Example 2: Data Lakehouse with GCS

``` r
library(deltaR)

# Configure GCS storage
gcs_options <- list(
  google_service_account_path = Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS")
)

# Create a new Delta table
sales_data <- data.frame(
  sale_id = 1:1000,
  store_id = sample(1:50, 1000, replace = TRUE),
  product_id = sample(1:200, 1000, replace = TRUE),
  sale_date = as.Date("2024-01-01") + sample(0:364, 1000, replace = TRUE),
  quantity = sample(1:20, 1000, replace = TRUE),
  total_amount = round(runif(1000, 10, 1000), 2)
)

write_deltalake(
  sales_data,
  "gs://analytics-lakehouse/sales/daily",
  partition_by = "sale_date",
  name = "daily_sales",
  description = "Daily sales transactions from all stores",
  storage_options = gcs_options
)

# Later: append new data
new_sales <- data.frame(
  sale_id = 1001:1100,
  store_id = sample(1:50, 100, replace = TRUE),
  product_id = sample(1:200, 100, replace = TRUE),
  sale_date = as.Date("2025-01-01") + sample(0:30, 100, replace = TRUE),
  quantity = sample(1:20, 100, replace = TRUE),
  total_amount = round(runif(100, 10, 1000), 2)
)

write_deltalake(
  new_sales,
  "gs://analytics-lakehouse/sales/daily",
  mode = "append",
  storage_options = gcs_options
)
```

### Example 3: Multi-Cloud Data Replication

``` r
library(deltaR)

# Source: AWS S3
s3_options <- list(
  aws_access_key_id = Sys.getenv("AWS_ACCESS_KEY_ID"),
  aws_secret_access_key = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
  aws_region = "us-east-1"
)

# Destination: Azure Blob Storage
azure_options <- list(
  azure_storage_account_name = Sys.getenv("AZURE_STORAGE_ACCOUNT"),
  azure_storage_account_key = Sys.getenv("AZURE_STORAGE_KEY")
)

# Read from S3
source_dt <- delta_table(
  "s3://source-bucket/important_data",
  storage_options = s3_options
)
data <- source_dt$to_arrow()

# Write to Azure
write_deltalake(
  data,
  "az://backup-container/important_data",
  mode = "overwrite",
  storage_options = azure_options
)
```

## Security Best Practices

### 1. Never Hardcode Credentials

Always use environment variables or secure secret management:

``` r
# Good: Use environment variables
storage_options <- list(
  aws_access_key_id = Sys.getenv("AWS_ACCESS_KEY_ID"),
  aws_secret_access_key = Sys.getenv("AWS_SECRET_ACCESS_KEY")
)

# Bad: Hardcoded credentials (never do this!)
# storage_options <- list(
#   aws_access_key_id = "AKIAIOSFODNN7EXAMPLE",
#   aws_secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
# )
```

### 2. Use IAM Roles When Possible

On cloud infrastructure, prefer IAM roles over access keys:

- **AWS**: Attach IAM roles to EC2 instances, Lambda functions, or ECS
  tasks
- **GCP**: Use service accounts attached to Compute Engine VMs or Cloud
  Run services
- **Azure**: Use Managed Identities for Azure VMs or Azure Functions

### 3. Principle of Least Privilege

Grant only the minimum permissions required:

``` json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::my-delta-bucket",
        "arn:aws:s3:::my-delta-bucket/*"
      ]
    }
  ]
}
```

### 4. Use Encryption

Enable encryption at rest and in transit:

- **S3**: Enable SSE-S3, SSE-KMS, or SSE-C encryption
- **GCS**: Enable Customer-Managed Encryption Keys (CMEK)
- **Azure**: Enable Storage Service Encryption (SSE)

## Troubleshooting

### Common Errors

#### “Access Denied” or “403 Forbidden”

- Verify credentials are correct
- Check IAM permissions include required actions (GetObject, PutObject,
  etc.)
- Ensure bucket/container policies allow access

#### “Bucket Not Found” or “404 Not Found”

- Verify the bucket/container name is correct
- Check the region matches where the bucket is located
- Ensure the URI format is correct for the cloud provider

#### “Connection Timeout”

- Check network connectivity to the cloud provider
- Verify firewall rules allow outbound HTTPS traffic
- For S3-compatible services, verify the endpoint URL is correct

### Debugging Tips

``` r
# Enable verbose logging (if available)
options(deltaR.verbose = TRUE)

# Test credentials with a simple operation
tryCatch({
  dt <- delta_table("s3://my-bucket/test_table", storage_options = storage_options)
  print(dt$version())
}, error = function(e) {
  message("Error: ", e$message)
})
```

## Acknowledgments

deltaR’s cloud storage support is powered by the
[object_store](https://github.com/apache/arrow-rs/tree/master/object_store)
crate from the Apache Arrow project and the
[delta-rs](https://github.com/delta-io/delta-rs) library. We thank the
maintainers and contributors of these projects for their excellent work.
