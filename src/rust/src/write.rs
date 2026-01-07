//! Write support for Delta Lake tables
//!
//! This module provides functionality to write data to Delta Lake tables
//! using the datafusion feature of deltalake-core.

use arrow::array::RecordBatch;
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow_extendr::from::FromArrowRobj;
use deltalake::kernel::{ArrayType, DataType as KernelDT, MapType, PrimitiveType, StructType};
use deltalake::protocol::SaveMode;
use deltalake::DeltaTable;
use extendr_api::prelude::*;
use std::str::FromStr;

use crate::{block_on, parse_storage_options, path_to_url};

/// Write data to a Delta Lake table
///
/// @param table_uri Path to the Delta table (will be created if it doesn't exist)
/// @param data Arrow data stream (nanoarrow_array_stream)
/// @param mode Save mode: "append", "overwrite", "error", or "ignore"
/// @param partition_by Column names to partition by (optional)
/// @param name Table name (optional, used when creating new table)
/// @param description Table description (optional, used when creating new table)
/// @param storage_options Storage backend options (optional)
/// @param schema_mode How to handle schema evolution: "overwrite" or "merge" (optional)
/// @export
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
) -> Result<List> {
    // Parse save mode
    let save_mode = SaveMode::from_str(mode).map_err(|e| Error::from(e.to_string()))?;

    // Convert R Arrow stream to RecordBatches
    let reader = ArrowArrayStreamReader::from_arrow_robj(&stream)
        .map_err(|e| Error::from(format!("Failed to read Arrow stream: {:?}", e)))?;

    let batches: Vec<RecordBatch> = reader
        .into_iter()
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::from(format!("Failed to collect record batches: {}", e)))?;

    if batches.is_empty() {
        return Err(Error::from("No data provided to write"));
    }

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

    // Build the write operation
    let mut write_builder = table.write(batches);

    // Set save mode
    write_builder = write_builder.with_save_mode(save_mode);

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

    // Execute the write
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

/// Create a new empty Delta Lake table
///
/// @param table_uri Path where the table will be created
/// @param schema Arrow schema for the table
/// @param partition_by Column names to partition by (optional)
/// @param name Table name (optional)
/// @param description Table description (optional)
/// @param storage_options Storage backend options (optional)
/// @param configuration Table configuration properties (optional)
/// @export
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

    // Convert Arrow fields to Delta kernel StructFields
    let columns: Vec<StructField> = arrow_schema
        .fields()
        .iter()
        .map(|f| {
            StructField::new(
                f.name().clone(),
                arrow_type_to_kernel(f.data_type()),
                f.is_nullable(),
            )
        })
        .collect();

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

/// Convert Arrow DataType to Kernel DataType
fn arrow_type_to_kernel(arrow_type: &arrow::datatypes::DataType) -> KernelDT {
    use arrow::datatypes::DataType as ArrowDT;

    match arrow_type {
        ArrowDT::Boolean => KernelDT::Primitive(PrimitiveType::Boolean),
        ArrowDT::Int8 => KernelDT::Primitive(PrimitiveType::Byte),
        ArrowDT::Int16 => KernelDT::Primitive(PrimitiveType::Short),
        ArrowDT::Int32 => KernelDT::Primitive(PrimitiveType::Integer),
        ArrowDT::Int64 => KernelDT::Primitive(PrimitiveType::Long),
        ArrowDT::UInt8 => KernelDT::Primitive(PrimitiveType::Byte),
        ArrowDT::UInt16 => KernelDT::Primitive(PrimitiveType::Short),
        ArrowDT::UInt32 => KernelDT::Primitive(PrimitiveType::Integer),
        ArrowDT::UInt64 => KernelDT::Primitive(PrimitiveType::Long),
        ArrowDT::Float16 => KernelDT::Primitive(PrimitiveType::Float),
        ArrowDT::Float32 => KernelDT::Primitive(PrimitiveType::Float),
        ArrowDT::Float64 => KernelDT::Primitive(PrimitiveType::Double),
        ArrowDT::Utf8 | ArrowDT::LargeUtf8 => KernelDT::Primitive(PrimitiveType::String),
        ArrowDT::Binary | ArrowDT::LargeBinary => KernelDT::Primitive(PrimitiveType::Binary),
        ArrowDT::Date32 | ArrowDT::Date64 => KernelDT::Primitive(PrimitiveType::Date),
        ArrowDT::Timestamp(_, Some(_)) => KernelDT::Primitive(PrimitiveType::Timestamp),
        ArrowDT::Timestamp(_, None) => KernelDT::Primitive(PrimitiveType::TimestampNtz),
        ArrowDT::Decimal128(precision, scale) => KernelDT::decimal(*precision, *scale as u8)
            .unwrap_or_else(|_| KernelDT::Primitive(PrimitiveType::Double)),
        ArrowDT::Decimal256(precision, scale) => KernelDT::decimal(*precision, *scale as u8)
            .unwrap_or_else(|_| KernelDT::Primitive(PrimitiveType::Double)),
        ArrowDT::List(field) | ArrowDT::LargeList(field) => {
            let element_type = arrow_type_to_kernel(field.data_type());
            KernelDT::Array(Box::new(ArrayType::new(element_type, field.is_nullable())))
        }
        ArrowDT::Map(field, _) => {
            // Map field contains a struct with key and value
            if let ArrowDT::Struct(fields) = field.data_type() {
                if fields.len() >= 2 {
                    let key_type = arrow_type_to_kernel(fields[0].data_type());
                    let value_type = arrow_type_to_kernel(fields[1].data_type());
                    return KernelDT::Map(Box::new(MapType::new(
                        key_type,
                        value_type,
                        fields[1].is_nullable(),
                    )));
                }
            }
            // Fallback
            KernelDT::Primitive(PrimitiveType::String)
        }
        ArrowDT::Struct(fields) => {
            let struct_fields: Vec<deltalake::kernel::StructField> = fields
                .iter()
                .map(|f| {
                    deltalake::kernel::StructField::new(
                        f.name().clone(),
                        arrow_type_to_kernel(f.data_type()),
                        f.is_nullable(),
                    )
                })
                .collect();
            match StructType::try_new(struct_fields) {
                Ok(st) => KernelDT::Struct(Box::new(st)),
                Err(_) => KernelDT::Primitive(PrimitiveType::String), // fallback
            }
        }
        // Default fallback for unsupported types
        _ => KernelDT::Primitive(PrimitiveType::String),
    }
}

// Export the module functions
extendr_module! {
    mod write;
    fn delta_write;
    fn delta_create;
}
