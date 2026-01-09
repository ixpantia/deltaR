//! MERGE support for Delta Lake tables
//!
//! This module provides functionality to merge data into Delta Lake tables
//! using delta-rs MergeBuilder. The merge configuration is built entirely
//! in R and passed to a single Rust function for execution.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow_extendr::from::FromArrowRobj;
use deltalake::arrow::array::RecordBatch;
use deltalake::arrow::array::RecordBatchReader;
use deltalake::datafusion::datasource::MemTable;
use deltalake::datafusion::prelude::*;
use deltalake::operations::merge::MergeBuilder;
use deltalake::DeltaTable;
use extendr_api::prelude::*;

use crate::{block_on, parse_storage_options, path_to_url};

/// Execute a Delta Lake MERGE operation
///
/// This function receives all merge configuration from R and executes
/// the merge in a single call, avoiding complex state management.
///
/// @param table_uri Path to the Delta table
/// @param source_stream Arrow data stream for source data
/// @param predicate Main merge predicate (e.g., "target.id = source.id")
/// @param source_alias Alias for source table in expressions
/// @param target_alias Alias for target table in expressions
/// @param matched_update_clauses List of update clauses for matched rows
/// @param matched_delete_clauses List of delete clauses for matched rows
/// @param not_matched_insert_clauses List of insert clauses for unmatched source rows
/// @param not_matched_by_source_update_clauses List of update clauses for unmatched target rows
/// @param not_matched_by_source_delete_clauses List of delete clauses for unmatched target rows
/// @param storage_options Storage backend options (optional)
#[extendr]
pub fn delta_merge_execute(
    table_uri: &str,
    source_stream: Robj,
    predicate: &str,
    source_alias: &str,
    target_alias: &str,
    matched_update_clauses: List,
    matched_delete_clauses: List,
    not_matched_insert_clauses: List,
    not_matched_by_source_update_clauses: List,
    not_matched_by_source_delete_clauses: List,
    storage_options: Nullable<List>,
) -> Result<List> {
    // Convert R Arrow stream to reader
    let reader = ArrowArrayStreamReader::from_arrow_robj(&source_stream)
        .map_err(|e| Error::from(format!("Failed to read Arrow stream: {:?}", e)))?;

    let schema = reader.schema();
    let boxed_reader: Box<dyn RecordBatchReader + Send + 'static> = Box::new(reader);

    // Parse URL and open table
    let url = path_to_url(table_uri).map_err(Error::from)?;

    let mut table = block_on(async {
        match storage_options {
            Nullable::NotNull(ref opts) => {
                let options = parse_storage_options(opts);
                DeltaTable::try_from_url_with_storage_options(url.clone(), options).await
            }
            Nullable::Null => DeltaTable::try_from_url(url.clone()).await,
        }
    })
    .map_err(|e| Error::from(e.to_string()))?;

    // Load the table
    block_on(async { table.load().await }).map_err(|e| Error::from(e.to_string()))?;

    // Collect all record batches from the source stream into memory
    let mut batches: Vec<RecordBatch> = Vec::new();
    let mut reader_box = boxed_reader;
    while let Some(batch_result) = reader_box.next() {
        let batch =
            batch_result.map_err(|e| Error::from(format!("Failed to read batch: {}", e)))?;
        batches.push(batch);
    }

    // Create a MemTable from the collected batches
    let mem_table = MemTable::try_new(schema.clone(), vec![batches])
        .map_err(|e| Error::from(format!("Failed to create memory table: {}", e)))?;

    // Create SessionContext and register the source table
    let ctx = SessionContext::new();
    block_on(async { ctx.register_table(source_alias, Arc::new(mem_table)) })
        .map_err(|e| Error::from(format!("Failed to register source table: {}", e)))?;

    // Get the source as a DataFrame
    let source_df = block_on(async { ctx.table(source_alias).await })
        .map_err(|e| Error::from(format!("Failed to get source DataFrame: {}", e)))?;

    // Get the table state - the table must be loaded
    let table_state = table
        .state
        .as_ref()
        .ok_or_else(|| Error::from("Table must be loaded before merge"))?;

    // Build merge operation - MergeBuilder::new takes Option<EagerSnapshot>
    let mut merge_builder = MergeBuilder::new(
        table.log_store(),
        Some(table_state.snapshot().clone()),
        predicate.to_string(),
        source_df,
    )
    .with_source_alias(source_alias)
    .with_target_alias(target_alias);

    // Add WHEN MATCHED UPDATE clauses
    for clause in matched_update_clauses.iter() {
        if let Some(clause_list) = clause.1.as_list() {
            merge_builder = add_matched_update_clause(merge_builder, clause_list)?;
        }
    }

    // Add WHEN MATCHED DELETE clauses
    for clause in matched_delete_clauses.iter() {
        if let Some(clause_list) = clause.1.as_list() {
            merge_builder = add_matched_delete_clause(merge_builder, clause_list)?;
        }
    }

    // Add WHEN NOT MATCHED INSERT clauses
    for clause in not_matched_insert_clauses.iter() {
        if let Some(clause_list) = clause.1.as_list() {
            merge_builder = add_not_matched_insert_clause(merge_builder, clause_list)?;
        }
    }

    // Add WHEN NOT MATCHED BY SOURCE UPDATE clauses
    for clause in not_matched_by_source_update_clauses.iter() {
        if let Some(clause_list) = clause.1.as_list() {
            merge_builder = add_not_matched_by_source_update_clause(merge_builder, clause_list)?;
        }
    }

    // Add WHEN NOT MATCHED BY SOURCE DELETE clauses
    for clause in not_matched_by_source_delete_clauses.iter() {
        if let Some(clause_list) = clause.1.as_list() {
            merge_builder = add_not_matched_by_source_delete_clause(merge_builder, clause_list)?;
        }
    }

    // Execute merge
    let (_table, metrics) = block_on(async { merge_builder.await })
        .map_err(|e| Error::from(format!("Merge failed: {}", e)))?;

    // Return metrics as R list
    Ok(list!(
        num_target_rows_inserted = metrics.num_target_rows_inserted as i64,
        num_target_rows_updated = metrics.num_target_rows_updated as i64,
        num_target_rows_deleted = metrics.num_target_rows_deleted as i64,
        num_target_files_added = metrics.num_target_files_added as i64,
        num_target_files_removed = metrics.num_target_files_removed as i64,
        num_target_rows_copied = metrics.num_target_rows_copied as i64,
        num_output_rows = metrics.num_output_rows as i64,
        execution_time_ms = metrics.execution_time_ms as i64
    ))
}

// ============================================================================
// Helper Functions to Add Clauses
// ============================================================================

/// Extract updates map from an R list
fn extract_updates(clause: &List) -> Result<HashMap<String, String>> {
    let mut updates = HashMap::new();

    // Try to get "updates" key from the list by iterating
    for (key, value) in clause.iter() {
        if key == "updates" {
            if let Some(updates_list) = value.as_list() {
                for (col_key, col_value) in updates_list.iter() {
                    if let Some(v) = col_value.as_str() {
                        updates.insert(col_key.to_string(), v.to_string());
                    }
                }
            }
            break;
        }
    }

    Ok(updates)
}

/// Extract optional predicate from an R list
fn extract_predicate(clause: &List) -> Option<String> {
    for (key, value) in clause.iter() {
        if key == "predicate" {
            return value.as_str().map(|s| s.to_string());
        }
    }
    None
}

/// Check if this is an "update_all" clause
fn is_update_all(clause: &List) -> bool {
    for (key, value) in clause.iter() {
        if key == "update_all" {
            return value.as_bool().unwrap_or(false);
        }
    }
    false
}

/// Check if this is an "insert_all" clause
fn is_insert_all(clause: &List) -> bool {
    for (key, value) in clause.iter() {
        if key == "insert_all" {
            return value.as_bool().unwrap_or(false);
        }
    }
    false
}

/// Add a WHEN MATCHED UPDATE clause to the builder
fn add_matched_update_clause(builder: MergeBuilder, clause: List) -> Result<MergeBuilder> {
    let predicate = extract_predicate(&clause);

    if is_update_all(&clause) {
        // UPDATE ALL - we need to get target schema and build updates for all columns
        // Since update_all isn't directly supported, we skip this for now
        // and require explicit column mappings
        return Err(Error::from(
            "when_matched_update_all is not yet supported. Please use when_matched_update with explicit column mappings."
        ));
    }

    // UPDATE with specific columns
    let updates = extract_updates(&clause)?;
    let result = match predicate {
        Some(p) => builder
            .when_matched_update(|mut update| {
                update = update.predicate(p);
                for (col, expr) in updates {
                    update = update.update(col, expr);
                }
                update
            })
            .map_err(|e| Error::from(e.to_string()))?,
        None => builder
            .when_matched_update(|mut update| {
                for (col, expr) in updates {
                    update = update.update(col, expr);
                }
                update
            })
            .map_err(|e| Error::from(e.to_string()))?,
    };
    Ok(result)
}

/// Add a WHEN MATCHED DELETE clause to the builder
fn add_matched_delete_clause(builder: MergeBuilder, clause: List) -> Result<MergeBuilder> {
    let predicate = extract_predicate(&clause);

    let result = match predicate {
        Some(p) => builder
            .when_matched_delete(|delete| delete.predicate(p))
            .map_err(|e| Error::from(e.to_string()))?,
        None => builder
            .when_matched_delete(|delete| delete)
            .map_err(|e| Error::from(e.to_string()))?,
    };
    Ok(result)
}

/// Add a WHEN NOT MATCHED INSERT clause to the builder
fn add_not_matched_insert_clause(builder: MergeBuilder, clause: List) -> Result<MergeBuilder> {
    let predicate = extract_predicate(&clause);

    if is_insert_all(&clause) {
        // INSERT ALL - not directly supported, require explicit mappings
        return Err(Error::from(
            "when_not_matched_insert_all is not yet supported. Please use when_not_matched_insert with explicit column mappings."
        ));
    }

    // INSERT with specific columns
    let updates = extract_updates(&clause)?;
    let result = match predicate {
        Some(p) => builder
            .when_not_matched_insert(|mut insert| {
                insert = insert.predicate(p);
                for (col, expr) in updates {
                    insert = insert.set(col, expr);
                }
                insert
            })
            .map_err(|e| Error::from(e.to_string()))?,
        None => builder
            .when_not_matched_insert(|mut insert| {
                for (col, expr) in updates {
                    insert = insert.set(col, expr);
                }
                insert
            })
            .map_err(|e| Error::from(e.to_string()))?,
    };
    Ok(result)
}

/// Add a WHEN NOT MATCHED BY SOURCE UPDATE clause to the builder
fn add_not_matched_by_source_update_clause(
    builder: MergeBuilder,
    clause: List,
) -> Result<MergeBuilder> {
    let predicate = extract_predicate(&clause);
    let updates = extract_updates(&clause)?;

    let result = match predicate {
        Some(p) => builder
            .when_not_matched_by_source_update(|mut update| {
                update = update.predicate(p);
                for (col, expr) in updates {
                    update = update.update(col, expr);
                }
                update
            })
            .map_err(|e| Error::from(e.to_string()))?,
        None => builder
            .when_not_matched_by_source_update(|mut update| {
                for (col, expr) in updates {
                    update = update.update(col, expr);
                }
                update
            })
            .map_err(|e| Error::from(e.to_string()))?,
    };
    Ok(result)
}

/// Add a WHEN NOT MATCHED BY SOURCE DELETE clause to the builder
fn add_not_matched_by_source_delete_clause(
    builder: MergeBuilder,
    clause: List,
) -> Result<MergeBuilder> {
    let predicate = extract_predicate(&clause);

    let result = match predicate {
        Some(p) => builder
            .when_not_matched_by_source_delete(|delete| delete.predicate(p))
            .map_err(|e| Error::from(e.to_string()))?,
        None => builder
            .when_not_matched_by_source_delete(|delete| delete)
            .map_err(|e| Error::from(e.to_string()))?,
    };
    Ok(result)
}

// Export module
extendr_module! {
    mod merge;
    fn delta_merge_execute;
}
