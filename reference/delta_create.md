# Create a new empty Delta Lake table

Create a new empty Delta Lake table

## Usage

``` r
delta_create(
  table_uri,
  schema,
  partition_by,
  name,
  description,
  storage_options,
  configuration
)
```

## Arguments

- table_uri:

  Path where the table will be created

- schema:

  Arrow schema for the table

- partition_by:

  Column names to partition by (optional)

- name:

  Table name (optional)

- description:

  Table description (optional)

- storage_options:

  Storage backend options (optional)

- configuration:

  Table configuration properties (optional)
