# Compact a Delta table

Compact files in a Delta table to reduce the number of small files and
improve query performance.

## Usage

``` r
compact(
  table,
  ...,
  target_size = NULL,
  max_concurrent_tasks = NULL,
  min_commit_interval_ms = NULL,
  partition_filters = NULL
)
```

## Arguments

- table:

  A DeltaTable object.

- ...:

  Additional arguments passed to methods.

- target_size:

  Numeric. Target size in bytes for compacted files.

- max_concurrent_tasks:

  Integer. Maximum number of concurrent tasks.

- min_commit_interval_ms:

  Numeric. Minimum interval between commits in milliseconds.

- partition_filters:

  Character vector. Filters to select partitions to compact (e.g.,
  c("date=2023-01-01")).

## Value

A list with compaction metrics.
