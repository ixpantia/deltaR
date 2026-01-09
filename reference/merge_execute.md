# Execute the MERGE operation

Executes the configured merge operation against the target Delta table.

## Usage

``` r
merge_execute(builder, ...)
```

## Arguments

- builder:

  A DeltaMergeBuilder object configured with merge clauses.

- ...:

  Additional arguments passed to methods.

## Value

A named list with merge metrics:

- `num_target_rows_inserted`: Number of rows inserted into target.

- `num_target_rows_updated`: Number of rows updated in target.

- `num_target_rows_deleted`: Number of rows deleted from target.

- `num_target_files_added`: Number of files added.

- `num_target_files_removed`: Number of files removed.

- `num_target_rows_copied`: Number of rows copied (unchanged).

- `num_output_rows`: Total number of rows in output.

- `execution_time_ms`: Execution time in milliseconds.

## Examples

``` r
if (FALSE) { # \dontrun{
result <- delta_merge("path/to/table", source, "target.id = source.id") |>
  when_matched_update(c(value = "source.value")) |>
  when_not_matched_insert(c(id = "source.id", value = "source.value")) |>
  merge_execute()

print(result)
# $num_target_rows_updated
# [1] 5
# $num_target_rows_inserted
# [1] 3
# ...
} # }
```
