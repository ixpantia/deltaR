//! Write support for Delta Lake tables
//!
//! This module provides functionality to write data to Delta Lake tables
//! using DataFusion's LogicalPlan infrastructure for better error handling
//! and streaming support, similar to the Python delta-rs implementation.

use std::any::Any;
use std::borrow::Cow;
use std::fmt;
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow_extendr::from::FromArrowRobj;
use deltalake::arrow::array::RecordBatchReader;
use deltalake::datafusion::catalog::{Session, TableProvider};
use deltalake::datafusion::datasource::provider_as_source;
use deltalake::datafusion::datasource::TableType;
use deltalake::datafusion::logical_expr::LogicalPlan;
use deltalake::datafusion::logical_expr::LogicalPlanBuilder;
use deltalake::datafusion::logical_expr::TableProviderFilterPushDown;
use deltalake::datafusion::physical_plan::memory::{LazyBatchGenerator, LazyMemoryExec};
use deltalake::datafusion::physical_plan::ExecutionPlan;
use deltalake::datafusion::prelude::Expr;
use deltalake::kernel::schema::cast_record_batch;
use deltalake::kernel::{ArrayType, DataType as KernelDT, MapType, PrimitiveType, StructType};
use deltalake::operations::write::WriteBuilder;
use deltalake::protocol::SaveMode;
use deltalake::DeltaTable;
use extendr_api::prelude::*;
use parking_lot::RwLock;
use std::str::FromStr;

use crate::{block_on, parse_storage_options, path_to_url};

/// Error type for type conversion failures
#[derive(Debug)]
pub struct TypeConversionError {
    message: String,
}

impl std::fmt::Display for TypeConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for TypeConversionError {}

impl From<TypeConversionError> for Error {
    fn from(e: TypeConversionError) -> Self {
        Error::from(e.message)
    }
}

// ============================================================================
// LazyTableProvider - DataFusion TableProvider for streaming data
// ============================================================================

/// A TableProvider that lazily generates batches from an Arrow stream.
/// This is modeled after the Python delta-rs implementation.
#[derive(Debug)]
struct LazyTableProvider {
    schema: SchemaRef,
    batches: Vec<Arc<RwLock<dyn LazyBatchGenerator>>>,
}

impl LazyTableProvider {
    fn try_new(
        schema: SchemaRef,
        batches: Vec<Arc<RwLock<dyn LazyBatchGenerator>>>,
    ) -> std::result::Result<Self, String> {
        Ok(LazyTableProvider { schema, batches })
    }
}

#[async_trait::async_trait]
impl TableProvider for LazyTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        None
    }

    async fn scan(
        &self,
        _session: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> deltalake::datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let plan: Arc<dyn ExecutionPlan> = Arc::new(LazyMemoryExec::try_new(
            self.schema(),
            self.batches.clone(),
        )?);

        // Note: For simplicity, we don't implement projection, filter, or limit pushdown here.
        // The WriteBuilder will handle these through its execution plan.
        // If needed, we could add FilterExec, ProjectionExec, GlobalLimitExec wrappers.
        let _ = (projection, limit);

        Ok(plan)
    }

    fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> deltalake::datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filter.len()])
    }
}

// ============================================================================
// ArrowStreamBatchGenerator - Lazy batch generator from Arrow stream
// ============================================================================

/// Wrapper around a RecordBatchReader that holds a Mutex for thread safety
struct ReaderWrapper {
    reader: Mutex<Box<dyn RecordBatchReader + Send + 'static>>,
}

impl fmt::Debug for ReaderWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReaderWrapper")
            .field("reader", &"<RecordBatchReader>")
            .finish()
    }
}

/// A LazyBatchGenerator implementation that reads from an Arrow stream
#[derive(Debug)]
struct ArrowStreamBatchGenerator {
    array_stream: ReaderWrapper,
}

impl fmt::Display for ArrowStreamBatchGenerator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ArrowStreamBatchGenerator {{ array_stream: {:?} }}",
            self.array_stream
        )
    }
}

impl ArrowStreamBatchGenerator {
    fn new(array_stream: Box<dyn RecordBatchReader + Send + 'static>) -> Self {
        Self {
            array_stream: ReaderWrapper {
                reader: Mutex::new(array_stream),
            },
        }
    }
}

impl LazyBatchGenerator for ArrowStreamBatchGenerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn generate_next_batch(&mut self) -> deltalake::datafusion::error::Result<Option<RecordBatch>> {
        let mut stream_reader = self.array_stream.reader.lock().map_err(|_| {
            deltalake::datafusion::error::DataFusionError::Execution(
                "Failed to lock the ArrowArrayStreamReader".to_string(),
            )
        })?;

        match stream_reader.next() {
            Some(Ok(record_batch)) => Ok(Some(record_batch)),
            Some(Err(err)) => Err(deltalake::datafusion::error::DataFusionError::ArrowError(
                Box::new(err),
                None,
            )),
            None => Ok(None), // End of stream
        }
    }
}

// ============================================================================
// Lazy Schema Casting
// ============================================================================

/// A lazy casting wrapper around a RecordBatchReader that casts each batch
/// to the target schema on-the-fly without loading all data into memory.
struct LazyCastReader {
    input: Box<dyn RecordBatchReader + Send + 'static>,
    target_schema: SchemaRef,
}

impl RecordBatchReader for LazyCastReader {
    fn schema(&self) -> SchemaRef {
        self.target_schema.clone()
    }
}

impl Iterator for LazyCastReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.input.next() {
            Some(Ok(batch)) => Some(
                cast_record_batch(&batch, self.target_schema.clone(), false, false)
                    .map_err(|e| ArrowError::CastError(e.to_string())),
            ),
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

/// Returns a boxed reader that lazily casts each batch to the provided schema.
/// If the schemas are already equal, returns the input reader unchanged.
fn maybe_lazy_cast_reader(
    input: Box<dyn RecordBatchReader + Send + 'static>,
    target_schema: SchemaRef,
) -> Box<dyn RecordBatchReader + Send + 'static> {
    if !input.schema().eq(&target_schema) {
        Box::new(LazyCastReader {
            input,
            target_schema,
        })
    } else {
        input
    }
}

/// Convert a RecordBatchReader into a LazyTableProvider for use with DataFusion
fn to_lazy_table(
    source: Box<dyn RecordBatchReader + Send + 'static>,
) -> std::result::Result<Arc<dyn TableProvider>, String> {
    let schema = source.schema();
    let arrow_stream_batch_generator: Arc<RwLock<dyn LazyBatchGenerator>> =
        Arc::new(RwLock::new(ArrowStreamBatchGenerator::new(source)));

    Ok(Arc::new(LazyTableProvider::try_new(
        schema.clone(),
        vec![arrow_stream_batch_generator],
    )?))
}

// ============================================================================
// Main Write Function
// ============================================================================

/// Write data to a Delta Lake table using WriteBuilder and LogicalPlan
///
/// This function uses DataFusion's execution framework to write data, providing:
/// - Better error handling through the DataFusion pipeline
/// - Lazy schema casting (casts each batch on-the-fly)
/// - Proper backpressure handling for large datasets
/// - Memory-efficient streaming writes
///
/// @param table_uri Path to the Delta table (will be created if it doesn't exist)
/// @param stream Arrow data stream (nanoarrow_array_stream)
/// @param mode Save mode: "append", "overwrite", "error", or "ignore"
/// @param partition_by Column names to partition by (optional)
/// @param name Table name (optional, used when creating new table)
/// @param description Table description (optional, used when creating new table)
/// @param storage_options Storage backend options (optional)
/// @param schema_mode How to handle schema evolution: "overwrite" or "merge" (optional)
/// @param target_file_size Target file size in bytes (optional)
#[extendr]
pub fn delta_write(
    table_uri: &str,
    stream: Robj,
    mode: &str,
    partition_by: Nullable<Vec<String>>,
    name: Nullable<&str>,
    description: Nullable<&str>,
    storage_options: Nullable<List>,
    schema_mode: Nullable<&str>,
    target_file_size: Nullable<i64>,
) -> Result<List> {
    // Parse save mode
    let save_mode = SaveMode::from_str(mode).map_err(|e| Error::from(e.to_string()))?;

    // Convert R Arrow stream to ArrowArrayStreamReader
    let reader = ArrowArrayStreamReader::from_arrow_robj(&stream)
        .map_err(|e| Error::from(format!("Failed to read Arrow stream: {:?}", e)))?;

    // Get the schema from the reader before we consume it
    let batch_schema = reader.schema();

    // Box the reader for use as a RecordBatchReader
    let boxed_reader: Box<dyn RecordBatchReader + Send + 'static> = Box::new(reader);

    // Parse URL
    let url = path_to_url(table_uri).map_err(Error::from)?;

    // Open or create the table
    let table = block_on(async {
        match storage_options {
            Nullable::NotNull(ref opts) => {
                let options = parse_storage_options(opts);
                DeltaTable::try_from_url_with_storage_options(url, options).await
            }
            Nullable::Null => DeltaTable::try_from_url(url).await,
        }
    })
    .map_err(|e| Error::from(e.to_string()))?;

    // Create WriteBuilder using the pattern from Python delta-rs
    let mut write_builder = WriteBuilder::new(
        table.log_store(),
        table.state.as_ref().map(|s| s.snapshot().clone()),
    )
    .with_save_mode(save_mode);

    // Apply lazy schema casting if needed and convert to LazyTableProvider
    let table_provider = to_lazy_table(maybe_lazy_cast_reader(boxed_reader, batch_schema.clone()))
        .map_err(|e| Error::from(format!("Failed to create table provider: {}", e)))?;

    // Build a LogicalPlan from the table provider
    let plan = LogicalPlanBuilder::scan("source", provider_as_source(table_provider), None)
        .map_err(|e| Error::from(format!("Failed to create logical plan: {}", e)))?
        .build()
        .map_err(|e| Error::from(format!("Failed to build logical plan: {}", e)))?;

    // Set the input execution plan
    write_builder = write_builder.with_input_execution_plan(Arc::new(plan));

    // Set partition columns if provided
    if let Nullable::NotNull(cols) = partition_by {
        write_builder = write_builder.with_partition_columns(cols);
    }

    // Set table name if provided
    if let Nullable::NotNull(n) = name {
        write_builder = write_builder.with_table_name(n);
    }

    // Set description if provided
    if let Nullable::NotNull(desc) = description {
        write_builder = write_builder.with_description(desc);
    }

    // Set schema mode if provided
    if let Nullable::NotNull(sm) = schema_mode {
        let schema_mode_enum = deltalake::operations::write::SchemaMode::from_str(sm)
            .map_err(|e| Error::from(e.to_string()))?;
        write_builder = write_builder.with_schema_mode(schema_mode_enum);
    }

    // Set target file size if provided
    if let Nullable::NotNull(size) = target_file_size {
        if size > 0 {
            write_builder = write_builder.with_target_file_size(size as usize);
        }
    }

    // Execute the write using DataFusion's async execution
    let table = block_on(async { write_builder.await })
        .map_err(|e| Error::from(format!("Write failed: {}", e)))?;

    // Return result info as a list
    let version = table.version().unwrap_or(-1);
    let num_files = table
        .get_file_uris()
        .map(|iter| iter.count() as i32)
        .unwrap_or(0);

    let mut result = List::new(2);
    result.set_elt(0, version.into_robj())?;
    result.set_elt(1, num_files.into_robj())?;
    result.set_names(["version", "num_files"])?;

    Ok(result)
}

// ============================================================================
// Table Creation
// ============================================================================

/// Create a new empty Delta Lake table
///
/// @param table_uri Path where the table will be created
/// @param schema Arrow schema for the table
/// @param partition_by Column names to partition by (optional)
/// @param name Table name (optional)
/// @param description Table description (optional)
/// @param storage_options Storage backend options (optional)
/// @param configuration Table configuration properties (optional)
#[extendr]
pub fn delta_create(
    table_uri: &str,
    schema: Robj,
    partition_by: Nullable<Vec<String>>,
    name: Nullable<&str>,
    description: Nullable<&str>,
    storage_options: Nullable<List>,
    configuration: Nullable<List>,
) -> Result<i64> {
    use arrow::datatypes::Schema as ArrowSchema;
    use arrow_extendr::from::FromArrowRobj;
    use deltalake::kernel::StructField;
    use deltalake::operations::create::CreateBuilder;
    use std::collections::HashMap;

    // Convert R Arrow schema to Rust Arrow schema
    let arrow_schema = ArrowSchema::from_arrow_robj(&schema)
        .map_err(|e| Error::from(format!("Failed to read Arrow schema: {:?}", e)))?;

    // Parse URL
    let url = path_to_url(table_uri).map_err(Error::from)?;

    // Parse storage options
    let opts: HashMap<String, String> = match storage_options {
        Nullable::NotNull(ref opts) => parse_storage_options(opts),
        Nullable::Null => HashMap::new(),
    };

    // Convert Arrow fields to Delta kernel StructFields with strict type mapping
    let columns: std::result::Result<Vec<StructField>, TypeConversionError> = arrow_schema
        .fields()
        .iter()
        .map(|f| {
            let kernel_type = arrow_type_to_kernel(f.data_type())?;
            Ok(StructField::new(
                f.name().clone(),
                kernel_type,
                f.is_nullable(),
            ))
        })
        .collect();

    let columns = columns?;

    // Build the create operation
    let mut create_builder = CreateBuilder::new()
        .with_location(url.to_string())
        .with_columns(columns);

    // Add storage options
    if !opts.is_empty() {
        create_builder = create_builder.with_storage_options(opts);
    }

    // Set partition columns if provided
    if let Nullable::NotNull(cols) = partition_by {
        create_builder = create_builder.with_partition_columns(cols);
    }

    // Set table name if provided
    if let Nullable::NotNull(n) = name {
        create_builder = create_builder.with_table_name(n);
    }

    // Set description if provided
    if let Nullable::NotNull(desc) = description {
        create_builder = create_builder.with_comment(desc);
    }

    // Set configuration if provided (as raw key-value pairs)
    if let Nullable::NotNull(ref config) = configuration {
        let config_map: HashMap<String, Option<String>> = config
            .iter()
            .filter_map(|(key, value)| {
                value
                    .as_str()
                    .map(|v| (key.to_string(), Some(v.to_string())))
            })
            .collect();
        create_builder = create_builder.with_configuration(config_map);
    }

    // Execute the create
    let table = block_on(async { create_builder.await })
        .map_err(|e| Error::from(format!("Create failed: {}", e)))?;

    Ok(table.version().unwrap_or(0))
}

// ============================================================================
// Type Conversion: Arrow -> Delta Kernel
// ============================================================================

/// Convert Arrow DataType to Kernel DataType with strict type mapping
///
/// This function returns a Result instead of silently falling back to String
/// for unsupported types. This ensures data integrity by failing fast when
/// encountering types that cannot be properly mapped to Delta Lake.
fn arrow_type_to_kernel(
    arrow_type: &arrow::datatypes::DataType,
) -> std::result::Result<KernelDT, TypeConversionError> {
    use arrow::datatypes::DataType as ArrowDT;
    use arrow::datatypes::TimeUnit;

    match arrow_type {
        // Boolean type
        ArrowDT::Boolean => Ok(KernelDT::Primitive(PrimitiveType::Boolean)),

        // Signed integer types
        ArrowDT::Int8 => Ok(KernelDT::Primitive(PrimitiveType::Byte)),
        ArrowDT::Int16 => Ok(KernelDT::Primitive(PrimitiveType::Short)),
        ArrowDT::Int32 => Ok(KernelDT::Primitive(PrimitiveType::Integer)),
        ArrowDT::Int64 => Ok(KernelDT::Primitive(PrimitiveType::Long)),

        // Unsigned integer types - map to signed equivalents (may lose precision for large values)
        ArrowDT::UInt8 => Ok(KernelDT::Primitive(PrimitiveType::Short)), // Promote to avoid overflow
        ArrowDT::UInt16 => Ok(KernelDT::Primitive(PrimitiveType::Integer)), // Promote to avoid overflow
        ArrowDT::UInt32 => Ok(KernelDT::Primitive(PrimitiveType::Long)), // Promote to avoid overflow
        ArrowDT::UInt64 => {
            // UInt64 cannot be safely represented in Delta Lake's Long (Int64)
            Err(TypeConversionError {
                message: format!(
                    "Unsupported Arrow type for Delta Lake: {:?}. \
                    UInt64 values may exceed the range of Delta Lake's Long type (Int64). \
                    Consider casting to Int64 if values are within range, or use Decimal128.",
                    arrow_type
                ),
            })
        }

        // Floating point types
        ArrowDT::Float16 => Ok(KernelDT::Primitive(PrimitiveType::Float)),
        ArrowDT::Float32 => Ok(KernelDT::Primitive(PrimitiveType::Float)),
        ArrowDT::Float64 => Ok(KernelDT::Primitive(PrimitiveType::Double)),

        // String types
        ArrowDT::Utf8 | ArrowDT::LargeUtf8 | ArrowDT::Utf8View => {
            Ok(KernelDT::Primitive(PrimitiveType::String))
        }

        // Binary types
        ArrowDT::Binary
        | ArrowDT::LargeBinary
        | ArrowDT::BinaryView
        | ArrowDT::FixedSizeBinary(_) => Ok(KernelDT::Primitive(PrimitiveType::Binary)),

        // Date types
        ArrowDT::Date32 | ArrowDT::Date64 => Ok(KernelDT::Primitive(PrimitiveType::Date)),

        // Timestamp types with timezone (Delta Lake Timestamp)
        ArrowDT::Timestamp(TimeUnit::Second, Some(_))
        | ArrowDT::Timestamp(TimeUnit::Millisecond, Some(_))
        | ArrowDT::Timestamp(TimeUnit::Microsecond, Some(_))
        | ArrowDT::Timestamp(TimeUnit::Nanosecond, Some(_)) => {
            Ok(KernelDT::Primitive(PrimitiveType::Timestamp))
        }

        // Timestamp types without timezone (Delta Lake TimestampNtz)
        ArrowDT::Timestamp(TimeUnit::Second, None)
        | ArrowDT::Timestamp(TimeUnit::Millisecond, None)
        | ArrowDT::Timestamp(TimeUnit::Microsecond, None)
        | ArrowDT::Timestamp(TimeUnit::Nanosecond, None) => {
            Ok(KernelDT::Primitive(PrimitiveType::TimestampNtz))
        }

        // Time types - Delta Lake doesn't have native time support
        ArrowDT::Time32(_) | ArrowDT::Time64(_) => Err(TypeConversionError {
            message: format!(
                "Unsupported Arrow type for Delta Lake: {:?}. \
                Delta Lake does not support standalone Time types. \
                Consider using Timestamp or storing as String/Int64.",
                arrow_type
            ),
        }),

        // Duration types - Delta Lake doesn't have native duration support
        ArrowDT::Duration(_) => Err(TypeConversionError {
            message: format!(
                "Unsupported Arrow type for Delta Lake: {:?}. \
                Delta Lake does not support Duration types. \
                Consider storing as Int64 (representing microseconds or your preferred unit).",
                arrow_type
            ),
        }),

        // Interval types - Delta Lake doesn't have native interval support
        ArrowDT::Interval(_) => Err(TypeConversionError {
            message: format!(
                "Unsupported Arrow type for Delta Lake: {:?}. \
                Delta Lake does not support Interval types. \
                Consider storing interval components as separate columns.",
                arrow_type
            ),
        }),

        // Decimal types
        ArrowDT::Decimal32(precision, scale) => KernelDT::decimal(*precision, *scale as u8)
            .map_err(|e| TypeConversionError {
                message: format!(
                    "Invalid Decimal32 parameters (precision: {}, scale: {}): {}",
                    precision, scale, e
                ),
            }),
        ArrowDT::Decimal64(precision, scale) => KernelDT::decimal(*precision, *scale as u8)
            .map_err(|e| TypeConversionError {
                message: format!(
                    "Invalid Decimal64 parameters (precision: {}, scale: {}): {}",
                    precision, scale, e
                ),
            }),
        ArrowDT::Decimal128(precision, scale) => KernelDT::decimal(*precision, *scale as u8)
            .map_err(|e| TypeConversionError {
                message: format!(
                    "Invalid Decimal128 parameters (precision: {}, scale: {}): {}",
                    precision, scale, e
                ),
            }),
        ArrowDT::Decimal256(precision, scale) => {
            // Decimal256 has higher precision than Delta Lake supports
            // Delta Lake Decimal supports up to precision 38
            if *precision > 38 {
                Err(TypeConversionError {
                    message: format!(
                        "Unsupported Arrow type for Delta Lake: Decimal256 with precision {}. \
                        Delta Lake supports Decimal with precision up to 38. \
                        Consider reducing precision or storing as String.",
                        precision
                    ),
                })
            } else {
                KernelDT::decimal(*precision, *scale as u8).map_err(|e| TypeConversionError {
                    message: format!(
                        "Invalid Decimal256 parameters (precision: {}, scale: {}): {}",
                        precision, scale, e
                    ),
                })
            }
        }

        // List types
        ArrowDT::List(field) | ArrowDT::LargeList(field) | ArrowDT::FixedSizeList(field, _) => {
            let element_type = arrow_type_to_kernel(field.data_type())?;
            Ok(KernelDT::Array(Box::new(ArrayType::new(
                element_type,
                field.is_nullable(),
            ))))
        }
        ArrowDT::ListView(field) | ArrowDT::LargeListView(field) => {
            let element_type = arrow_type_to_kernel(field.data_type())?;
            Ok(KernelDT::Array(Box::new(ArrayType::new(
                element_type,
                field.is_nullable(),
            ))))
        }

        // Map type
        ArrowDT::Map(field, _) => {
            // Map field contains a struct with key and value
            if let ArrowDT::Struct(fields) = field.data_type() {
                if fields.len() >= 2 {
                    let key_type = arrow_type_to_kernel(fields[0].data_type())?;
                    let value_type = arrow_type_to_kernel(fields[1].data_type())?;
                    return Ok(KernelDT::Map(Box::new(MapType::new(
                        key_type,
                        value_type,
                        fields[1].is_nullable(),
                    ))));
                }
            }
            Err(TypeConversionError {
                message: format!(
                    "Invalid Map type structure: {:?}. \
                    Expected Map field to contain a Struct with at least 2 fields (key, value).",
                    arrow_type
                ),
            })
        }

        // Struct type
        ArrowDT::Struct(fields) => {
            let struct_fields: std::result::Result<Vec<deltalake::kernel::StructField>, _> = fields
                .iter()
                .map(|f| {
                    let kernel_type = arrow_type_to_kernel(f.data_type())?;
                    Ok(deltalake::kernel::StructField::new(
                        f.name().clone(),
                        kernel_type,
                        f.is_nullable(),
                    ))
                })
                .collect();
            let struct_fields = struct_fields?;

            StructType::try_new(struct_fields)
                .map(|st| KernelDT::Struct(Box::new(st)))
                .map_err(|e| TypeConversionError {
                    message: format!("Failed to create Delta Struct type: {}", e),
                })
        }

        // Union types - Delta Lake doesn't support unions
        ArrowDT::Union(_, _) => Err(TypeConversionError {
            message: format!(
                "Unsupported Arrow type for Delta Lake: {:?}. \
                Delta Lake does not support Union types. \
                Consider restructuring your data or using a Struct with nullable fields.",
                arrow_type
            ),
        }),

        // Dictionary types - should be decoded before writing
        ArrowDT::Dictionary(_, value_type) => {
            // Dictionary encoding is a storage optimization in Arrow
            // Delta/Parquet will handle its own encoding, so we map to the value type
            arrow_type_to_kernel(value_type)
        }

        // RunEndEncoded - should be decoded before writing
        ArrowDT::RunEndEncoded(_, field) => {
            // Run-end encoding is a storage optimization in Arrow
            // We map to the underlying value type
            arrow_type_to_kernel(field.data_type())
        }

        // Null type
        ArrowDT::Null => Err(TypeConversionError {
            message: "Unsupported Arrow type for Delta Lake: Null. \
                Delta Lake does not support columns with only null values. \
                Consider removing null-only columns or assigning a concrete type."
                .to_string(),
        }),
    }
}

// Export the module functions
extendr_module! {
    mod write;
    fn delta_write;
    fn delta_create;
}
