# MERGE Operations in deltaR

## Introduction

Delta Lake’s MERGE operation allows you to perform sophisticated
upserts, combining insert, update, and delete operations in a single
atomic transaction. This is particularly useful for:

- **Upserts**: Update existing records and insert new ones
- **Change Data Capture (CDC)**: Applying incremental changes to a table
- **Slowly Changing Dimensions (SCD)**: Managing historical data in data
  warehouses
- **Data synchronization**: Keeping tables in sync with source systems

This vignette demonstrates how to use the
[`delta_merge()`](https://ixpantia.github.io/deltaR/reference/delta_merge.md)
function and its associated clause builders in deltaR.

## Basic Usage

The merge operation follows a builder pattern:

1.  Start with
    [`delta_merge()`](https://ixpantia.github.io/deltaR/reference/delta_merge.md)
    to specify the target table, source data, and join predicate

2.  Add one or more clauses (`when_matched_*`, `when_not_matched_*`)

3.  Execute with
    [`merge_execute()`](https://ixpantia.github.io/deltaR/reference/merge_execute.md)

``` r
library(deltaR)
```

### Simple Update (WHEN MATCHED)

Update existing records when a match is found:

``` r
# Create target table
target <- data.frame(
  id = 1:3,
  name = c("Alice", "Bob", "Charlie"),
  score = c(100, 85, 90)
)
write_deltalake(target, "path/to/table")

# Source data with updated scores
source <- data.frame(
  id = c(1, 2),
  name = c("Alice", "Bob"),
  score = c(105, 88)
)

# Update matched rows
result <- delta_merge("path/to/table", source, "target.id = source.id") |>
  when_matched_update(c(score = "source.score")) |>
  merge_execute()

# Result metrics
print(result)
# $num_target_rows_updated
# [1] 2
```

### Simple Insert (WHEN NOT MATCHED)

Insert new records when no match is found in the target:

``` r
# Source with new records
source <- data.frame(
  id = c(4, 5),
  name = c("Diana", "Eve"),
  score = c(92, 78)
)

# Insert non-matching rows
result <- delta_merge("path/to/table", source, "target.id = source.id") |>
  when_not_matched_insert(c(
    id = "source.id",
    name = "source.name",
    score = "source.score"
  )) |>
  merge_execute()

print(result$num_target_rows_inserted)
# [1] 2
```

### Upsert (Update + Insert)

The most common pattern - update existing records and insert new ones:

``` r
source <- data.frame(
  id = c(1, 6),       # 1 exists, 6 is new
  name = c("Alice Updated", "Frank"),
  score = c(110, 82)
)

result <- delta_merge("path/to/table", source, "target.id = source.id") |>
  when_matched_update(c(
    name = "source.name",
    score = "source.score"
  )) |>
  when_not_matched_insert(c(
    id = "source.id",
    name = "source.name",
    score = "source.score"
  )) |>
  merge_execute()

print(result)
# $num_target_rows_updated
# [1] 1
# $num_target_rows_inserted
# [1] 1
```

## Conditional Operations

All clause types support optional predicates for conditional execution.

### Conditional Update

Only update rows that meet additional criteria:

``` r
source <- data.frame(
  id = c(1, 2, 3),
  score = c(50, 95, 85)
)

# Only update if the new score is higher
result <- delta_merge("path/to/table", source, "target.id = source.id") |>
  when_matched_update(
    updates = c(score = "source.score"),
    predicate = "source.score > target.score"
  ) |>
  merge_execute()
```

### Conditional Delete

Delete matched rows based on a condition:

``` r
source <- data.frame(
  id = c(1, 2, 3),
  should_delete = c(FALSE, TRUE, FALSE)
)

result <- delta_merge("path/to/table", source, "target.id = source.id") |>
  when_matched_delete(predicate = "source.should_delete = true") |>
  merge_execute()
```

### Conditional Insert

Only insert rows that meet certain criteria:

``` r
source <- data.frame(
  id = c(10, 11, 12),
  name = c("Test1", "Valid", "Test2"),
  score = c(50, 85, 60)
)

# Only insert rows with score >= 70
result <- delta_merge("path/to/table", source, "target.id = source.id") |>
  when_not_matched_insert(
    updates = c(
      id = "source.id",
      name = "source.name",
      score = "source.score"
    ),
    predicate = "source.score >= 70"
  ) |>
  merge_execute()
```

## Handling Unmatched Target Rows

Use `when_not_matched_by_source_*` clauses to handle rows in the target
that have no corresponding row in the source.

### Delete Stale Records

Remove target rows that don’t exist in the source (useful for full
sync):

``` r
# Source represents the complete current state
source <- data.frame(
  id = c(1, 2),  # id 3 no longer exists
  name = c("Alice", "Bob"),
  score = c(100, 85)
)

result <- delta_merge("path/to/table", source, "target.id = source.id") |>
  when_not_matched_by_source_delete() |>
  merge_execute()

# Row with id=3 will be deleted
```

### Update Stale Records

Mark unmatched target rows as inactive instead of deleting:

``` r
# Assuming target has an 'active' column
result <- delta_merge("path/to/table", source, "target.id = source.id") |>
  when_not_matched_by_source_update(c(active = "'false'")) |>
  merge_execute()
```

## Complex Merge Operations

You can combine multiple clauses for sophisticated data transformations.

### Full CDC Pattern

Handle inserts, updates, and deletes in one operation:

``` r
# CDC source with operation type
cdc_data <- data.frame(
  id = c(1, 2, 7),
  name = c("Alice Updated", "Bob", "Grace"),
  score = c(115, 85, 88),
  operation = c("U", "D", "I")  # Update, Delete, Insert
)

result <- delta_merge("path/to/table", cdc_data, "target.id = source.id") |>
  # Handle updates
  when_matched_update(
    updates = c(name = "source.name", score = "source.score"),
    predicate = "source.operation = 'U'"
  ) |>
  # Handle deletes
  when_matched_delete(predicate = "source.operation = 'D'") |>
  # Handle inserts
  when_not_matched_insert(
    updates = c(
      id = "source.id",
      name = "source.name",
      score = "source.score"
    ),
    predicate = "source.operation = 'I'"
  ) |>
  merge_execute()
```

### Multiple Update Clauses

Apply different updates based on conditions:

``` r
source <- data.frame(
  id = c(1, 2, 3),
  score = c(95, 40, 75),
  bonus = c(10, 5, 8)
)

result <- delta_merge("path/to/table", source, "target.id = source.id") |>
  # High performers get score + bonus
  when_matched_update(
    updates = c(score = "source.score + source.bonus"),
    predicate = "source.score >= 90"
  ) |>
  # Others just get the score
  when_matched_update(
    updates = c(score = "source.score"),
    predicate = "source.score < 90"
  ) |>
  merge_execute()
```

## Using with DeltaTable Objects

You can pass a `DeltaTable` object instead of a path:

``` r
dt <- delta_table("path/to/table")

result <- delta_merge(dt, source, "target.id = source.id") |>
  when_matched_update(c(score = "source.score")) |>
  merge_execute()
```

## Merge Metrics

[`merge_execute()`](https://ixpantia.github.io/deltaR/reference/merge_execute.md)
returns a list of metrics about the operation:

``` r
result <- delta_merge("path/to/table", source, "target.id = source.id") |>
  when_matched_update(c(score = "source.score")) |>
  when_not_matched_insert(c(
    id = "source.id",
    name = "source.name",
    score = "source.score"
  )) |>
  merge_execute()

# Available metrics:
result$num_target_rows_inserted   # New rows added
result$num_target_rows_updated    # Existing rows modified
result$num_target_rows_deleted    # Rows removed
result$num_target_files_added     # New parquet files created
result$num_target_files_removed   # Old parquet files replaced
result$num_target_rows_copied     # Unchanged rows copied to new files
result$num_output_rows            # Total rows in output
result$execution_time_ms          # Operation duration
```

## Expression Syntax

Update expressions use SQL-like syntax:

``` r
# Column reference
c(score = "source.score")

# Arithmetic
c(score = "source.score + 10")
c(total = "source.base + source.bonus")

# String literals (use single quotes inside)
c(status = "'active'")
c(name = "'Unknown'")

# CASE expressions
c(grade = "CASE WHEN source.score >= 90 THEN 'A' ELSE 'B' END")

# NULL handling
c(notes = "COALESCE(source.notes, 'N/A')")
```

## Best Practices

1.  **Use meaningful predicates**: The join predicate should uniquely
    identify records. Using non-unique keys can lead to unexpected
    results.

2.  **Order clauses carefully**: When using multiple clauses of the same
    type, predicates are evaluated in order. Place more specific
    predicates first.

3.  **Test with small data**: Always test merge logic on a small dataset
    before running on production data.

4.  **Monitor metrics**: Check the returned metrics to verify the
    operation behaved as expected.

5.  **Consider partitioning**: For large tables, ensure your predicate
    can leverage partition pruning for better performance.

## Current Limitations

- [`when_matched_update_all()`](https://ixpantia.github.io/deltaR/reference/when_matched_update_all.md)
  and
  [`when_not_matched_insert_all()`](https://ixpantia.github.io/deltaR/reference/when_not_matched_insert_all.md)
  are not yet fully supported. Use explicit column mappings instead.

## See Also

- [`delta_merge()`](https://ixpantia.github.io/deltaR/reference/delta_merge.md) -
  Start a merge operation
- [`when_matched_update()`](https://ixpantia.github.io/deltaR/reference/when_matched_update.md) -
  Update matched rows
- [`when_matched_delete()`](https://ixpantia.github.io/deltaR/reference/when_matched_delete.md) -
  Delete matched rows
- [`when_not_matched_insert()`](https://ixpantia.github.io/deltaR/reference/when_not_matched_insert.md) -
  Insert unmatched source rows
- [`when_not_matched_by_source_update()`](https://ixpantia.github.io/deltaR/reference/when_not_matched_by_source_update.md) -
  Update unmatched target rows
- [`when_not_matched_by_source_delete()`](https://ixpantia.github.io/deltaR/reference/when_not_matched_by_source_delete.md) -
  Delete unmatched target rows
- [`merge_execute()`](https://ixpantia.github.io/deltaR/reference/merge_execute.md) -
  Execute the merge operation
