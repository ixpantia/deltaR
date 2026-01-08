mod write;

use arrow_extendr::to::IntoArrowRobj;
use deltalake::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    TimeUnit as ArrowTimeUnit,
};
use deltalake::kernel::{DataType as KernelDataType, PrimitiveType, StructField, StructType};
use deltalake::DeltaTable;
use extendr_api::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Runtime;

/// Register cloud storage handlers (GCS, S3, Azure) for deltalake
/// Called from R's .onLoad to enable cloud storage support
/// @export
#[extendr]
fn register_cloud_handlers() {
    // Register GCS handler
    deltalake::gcp::register_handlers(None);
    // Register S3 handler
    deltalake::aws::register_handlers(None);
    // Register Azure handler
    deltalake::azure::register_handlers(None);
}

/// Convert a kernel DataType to an Arrow DataType
fn kernel_type_to_arrow(kernel_type: &KernelDataType) -> ArrowDataType {
    match kernel_type {
        KernelDataType::Primitive(p) => match p {
            PrimitiveType::String => ArrowDataType::Utf8,
            PrimitiveType::Long => ArrowDataType::Int64,
            PrimitiveType::Integer => ArrowDataType::Int32,
            PrimitiveType::Short => ArrowDataType::Int16,
            PrimitiveType::Byte => ArrowDataType::Int8,
            PrimitiveType::Float => ArrowDataType::Float32,
            PrimitiveType::Double => ArrowDataType::Float64,
            PrimitiveType::Boolean => ArrowDataType::Boolean,
            PrimitiveType::Binary => ArrowDataType::Binary,
            PrimitiveType::Date => ArrowDataType::Date32,
            PrimitiveType::Timestamp => {
                ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, Some("UTC".into()))
            }
            PrimitiveType::TimestampNtz => {
                ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, None)
            }
            PrimitiveType::Decimal(decimal_type) => {
                ArrowDataType::Decimal128(decimal_type.precision(), decimal_type.scale() as i8)
            }
        },
        KernelDataType::Array(arr) => {
            let inner_type = kernel_type_to_arrow(arr.element_type());
            ArrowDataType::List(Arc::new(ArrowField::new(
                "item",
                inner_type,
                arr.contains_null(),
            )))
        }
        KernelDataType::Map(map) => {
            let key_type = kernel_type_to_arrow(map.key_type());
            let value_type = kernel_type_to_arrow(map.value_type());
            let entries_field = ArrowField::new(
                "entries",
                ArrowDataType::Struct(
                    vec![
                        ArrowField::new("key", key_type, false),
                        ArrowField::new("value", value_type, map.value_contains_null()),
                    ]
                    .into(),
                ),
                false,
            );
            ArrowDataType::Map(Arc::new(entries_field), false)
        }
        KernelDataType::Struct(s) => {
            let fields: Vec<ArrowField> = s.fields().map(|f| kernel_field_to_arrow(f)).collect();
            ArrowDataType::Struct(fields.into())
        }
        KernelDataType::Variant(_) => {
            // Variant type is stored as a struct with value and metadata
            ArrowDataType::Utf8 // Fallback to string representation
        }
    }
}

/// Convert a kernel StructField to an Arrow Field
fn kernel_field_to_arrow(field: &StructField) -> ArrowField {
    let arrow_type = kernel_type_to_arrow(field.data_type());
    ArrowField::new(field.name(), arrow_type, field.is_nullable())
}

/// Convert a kernel StructType (schema) to an Arrow Schema
fn kernel_schema_to_arrow(schema: &StructType) -> ArrowSchema {
    let fields: Vec<ArrowField> = schema.fields().map(|f| kernel_field_to_arrow(f)).collect();
    ArrowSchema::new(fields)
}

// Thread-local runtime for async operations to bridge R (sync) and Delta-rs (async)
thread_local! {
    static RUNTIME: Runtime = Runtime::new().expect("Failed to create Tokio runtime");
}

/// Execute an async block in the thread-local runtime
pub(crate) fn block_on<F>(future: F) -> F::Output
where
    F: std::future::Future,
{
    RUNTIME.with(|rt| rt.block_on(future))
}

/// Helper to parse storage options from R List
pub(crate) fn parse_storage_options(opts: &List) -> HashMap<String, String> {
    let mut options: HashMap<String, String> = HashMap::new();
    for (key, value) in opts.iter() {
        if let Some(v) = value.as_str() {
            options.insert(key.to_string(), v.to_string());
        }
    }
    options
}

/// Helper to convert a path string to URL
pub(crate) fn path_to_url(path: &str) -> std::result::Result<url::Url, String> {
    // Try parsing as URL first
    if let Ok(url) = url::Url::parse(path) {
        return Ok(url);
    }

    // Treat as local path
    let path_buf = std::path::Path::new(path);
    let canonical = path_buf
        .canonicalize()
        .unwrap_or_else(|_| path_buf.to_path_buf());

    url::Url::from_file_path(&canonical)
        .map_err(|_| format!("Failed to create URL from path: {}", path))
}

/// A wrapper around deltalake::DeltaTable
#[derive(Debug, Clone)]
#[extendr]
pub struct DeltaTableInternal {
    inner: DeltaTable,
}

#[extendr]
impl DeltaTableInternal {
    /// Get the current version of the Delta Table
    fn version(&self) -> i64 {
        self.inner.version().unwrap_or(-1)
    }

    /// Get the URI of the Delta Table
    fn uri(&self) -> String {
        self.inner.table_url().to_string()
    }

    /// Get the list of active file URIs in the current snapshot
    fn get_files(&self) -> Result<Vec<String>> {
        let file_uris: Vec<String> = self
            .inner
            .get_file_uris()
            .map_err(|e| Error::from(e.to_string()))?
            .collect();
        Ok(file_uris)
    }

    /// Get the number of files in the current snapshot
    fn num_files(&self) -> i64 {
        self.inner
            .get_file_uris()
            .map(|iter| iter.count() as i64)
            .unwrap_or(0)
    }

    /// Get table metadata as a list
    fn metadata(&self) -> Result<List> {
        let snapshot = self
            .inner
            .snapshot()
            .map_err(|e| Error::from(e.to_string()))?;
        let metadata = snapshot.metadata();

        let mut result = List::new(6);

        // Set values
        result.set_elt(0, metadata.id().to_string().into_robj())?;
        result.set_elt(
            1,
            metadata
                .name()
                .map(|s| s.to_string())
                .unwrap_or_default()
                .into_robj(),
        )?;
        result.set_elt(
            2,
            metadata
                .description()
                .map(|s| s.to_string())
                .unwrap_or_default()
                .into_robj(),
        )?;
        result.set_elt(3, metadata.partition_columns().to_vec().into_robj())?;
        result.set_elt(4, metadata.created_time().unwrap_or(0).into_robj())?;

        // Configuration as a named character vector
        let config_pairs: Vec<(String, String)> = metadata
            .configuration()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        let config_values: Vec<String> = config_pairs
            .iter()
            .map(|(_, v): &(String, String)| v.clone())
            .collect();
        let config_names: Vec<String> = config_pairs
            .iter()
            .map(|(k, _): &(String, String)| k.clone())
            .collect();
        let mut config_robj = config_values.into_robj();
        if !config_names.is_empty() {
            config_robj.set_names(config_names).ok();
        }
        result.set_elt(5, config_robj)?;

        // Set names
        result.set_names([
            "id",
            "name",
            "description",
            "partition_columns",
            "created_time",
            "configuration",
        ])?;

        Ok(result)
    }

    /// Get table schema as an Arrow schema (returns as Robj)
    fn schema(&self) -> Result<Robj> {
        let snapshot = self
            .inner
            .snapshot()
            .map_err(|e| Error::from(e.to_string()))?;
        let delta_schema = snapshot.schema();

        // Convert Delta schema to Arrow schema using our conversion function
        let arrow_schema = kernel_schema_to_arrow(delta_schema.as_ref());
        arrow_schema
            .into_arrow_robj()
            .map_err(|e| Error::from(e.to_string()))
    }

    /// Get commit history
    fn history(&self, limit: Nullable<i64>) -> Result<Robj> {
        let limit_val = match limit {
            Nullable::NotNull(l) => Some(l as usize),
            Nullable::Null => None,
        };

        let history: Vec<_> = block_on(async { self.inner.history(limit_val).await })
            .map_err(|e| Error::from(e.to_string()))?
            .collect();

        // Convert history to vectors for dataframe construction
        let n = history.len();
        let mut versions: Vec<i64> = Vec::with_capacity(n);
        let mut timestamps: Vec<i64> = Vec::with_capacity(n);
        let mut operations: Vec<String> = Vec::with_capacity(n);
        let mut user_ids: Vec<String> = Vec::with_capacity(n);
        let mut user_names: Vec<String> = Vec::with_capacity(n);

        for (idx, commit) in history.into_iter().enumerate() {
            // Version is inferred from position in history (most recent first)
            versions.push(idx as i64);
            timestamps.push(commit.timestamp.unwrap_or(0));
            operations.push(commit.operation.unwrap_or_default());
            user_ids.push(commit.user_id.unwrap_or_default());
            user_names.push(commit.user_name.unwrap_or_default());
        }

        // Create a data.frame
        let df = data_frame!(
            version = versions,
            timestamp = timestamps,
            operation = operations,
            user_id = user_ids,
            user_name = user_names
        );

        Ok(df.into_robj())
    }

    /// Load a specific version of the table
    fn load_version(&mut self, version: i64) -> Result<()> {
        block_on(async { self.inner.load_version(version).await })
            .map_err(|e| Error::from(e.to_string()))?;
        Ok(())
    }

    /// Load table at a specific datetime (ISO 8601 format)
    fn load_datetime(&mut self, datetime_str: &str) -> Result<()> {
        let datetime = chrono::DateTime::parse_from_rfc3339(datetime_str)
            .map_err(|e| Error::from(format!("Invalid datetime format: {}", e)))?;

        block_on(async {
            self.inner
                .load_with_datetime(datetime.with_timezone(&chrono::Utc))
                .await
        })
        .map_err(|e| Error::from(e.to_string()))?;

        Ok(())
    }

    /// Vacuum the table (remove old files)
    fn vacuum(
        &self,
        retention_hours: Nullable<f64>,
        dry_run: bool,
        enforce_retention_duration: bool,
    ) -> Result<Vec<String>> {
        let (_, metrics) = block_on(async {
            let mut vacuum_builder = self.inner.clone().vacuum();

            if let Nullable::NotNull(hours) = retention_hours {
                vacuum_builder =
                    vacuum_builder.with_retention_period(chrono::Duration::hours(hours as i64));
            }

            vacuum_builder = vacuum_builder.with_dry_run(dry_run);
            vacuum_builder =
                vacuum_builder.with_enforce_retention_duration(enforce_retention_duration);

            vacuum_builder.await
        })
        .map_err(|e| Error::from(e.to_string()))?;

        Ok(metrics.files_deleted)
    }

    /// Get partition columns
    fn partition_columns(&self) -> Result<Vec<String>> {
        let snapshot = self
            .inner
            .snapshot()
            .map_err(|e| Error::from(e.to_string()))?;
        Ok(snapshot.metadata().partition_columns().to_vec())
    }
}

/// Open a Delta Table at the specified path
/// @export
#[extendr]
fn delta_table_open(path: &str, storage_options: Nullable<List>) -> Result<DeltaTableInternal> {
    let url = path_to_url(path).map_err(Error::from)?;

    let mut table = block_on(async {
        match storage_options {
            Nullable::NotNull(ref opts) => {
                let options = parse_storage_options(opts);
                DeltaTable::try_from_url_with_storage_options(url, options).await
            }
            Nullable::Null => DeltaTable::try_from_url(url).await,
        }
    })
    .map_err(|e| Error::from(e.to_string()))?;

    // Load the table
    block_on(async { table.load().await }).map_err(|e| Error::from(e.to_string()))?;

    Ok(DeltaTableInternal { inner: table })
}

/// Check if a path is a Delta Table
/// @export
#[extendr]
fn is_delta_table(path: &str, storage_options: Nullable<List>) -> bool {
    let url = match path_to_url(path) {
        Ok(u) => u,
        Err(_) => return false,
    };

    block_on(async {
        let result = match storage_options {
            Nullable::NotNull(ref opts) => {
                let options = parse_storage_options(opts);
                DeltaTable::try_from_url_with_storage_options(url, options).await
            }
            Nullable::Null => DeltaTable::try_from_url(url).await,
        };

        match result {
            Ok(mut t) => t.load().await.is_ok(),
            Err(_) => false,
        }
    })
}

// Macro to generate exports.
// This ensures exported functions are registered with R.
extendr_module! {
    mod deltaR;
    use write;
    impl DeltaTableInternal;
    fn register_cloud_handlers;
    fn delta_table_open;
    fn is_delta_table;
}
