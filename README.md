# ExIcebergPort

Apache Iceberg port for Elixir applications, providing SQL interface through Apache Spark

## Requirements

- Java 11 (OpenJDK 11 recommended)
- Apache Spark 3.4.1
- Scala 2.13.12
- SBT (Scala Build Tool)

Before running the application, ensure you have Java 11 set up correctly:

```bash
export JAVA_HOME=/path/to/your/java11
export PATH="$JAVA_HOME/bin:$PATH"
```

You can verify your Java version with:

```bash
java -version
```

## Building

Before using the library, you need to build the JVM component:

```bash
make jvm
```

## Development

To start the application in development mode with IEx shell:

```bash
make dev
```

## Usage

### Start the Port

To start the port, use the following command:

```elixir
ExIcebergPort.start_link(
  warehouse_path: "LOCAL_PATH",
  catalog_name: "local",
)
```

### Create table

To create a table, use the following command:

```elixir
ExIcebergPort.query("CREATE TABLE IF NOT EXISTS local.db.my_table (id INT, name STRING, age INT) USING iceberg")
```

### Insert data

To insert data, use the following command:

```elixir
ExIcebergPort.query("insert into local.db.my_table values (1, 'John', 30), (2, 'Jane', 25), (3, 'Bob', 35)")
```

### Select rows

To select rows, use the following command:

```elixir
ExIcebergPort.query("SELECT * FROM local.db.my_table")
```

### Table Maintenance Operations

#### List Snapshots

View all snapshots (versions) of a table:

```elixir
ExIcebergPort.query("SELECT * FROM local.db.my_table.snapshots")
```

#### Compact Table Files

To optimize table performance by rewriting and compacting the table data:

```elixir
ExIcebergPort.query("""
  INSERT OVERWRITE TABLE local.db.my_table
  SELECT * FROM local.db.my_table
""")
```

This operation will rewrite the table data, which helps to:

- Compact small files into larger ones
- Remove deleted records
- Optimize the table's physical layout

#### Expire Old Snapshots

Remove old snapshots to free up storage space. This operation is safe as it only removes snapshots that are no longer needed for time travel queries:

```elixir
ExIcebergPort.query("""
  CALL catalog_name.system.expire_snapshots(
    table => 'local.db.my_table',
    older_than => TIMESTAMP '2025-05-24 00:00:00'
  )
""")
```

#### Remove Orphan Files

After expiring snapshots, you can physically delete unreferenced files to reclaim storage space. This operation should be run after `expire_snapshots`:

```elixir
ExIcebergPort.query("""
  CALL catalog_name.system.remove_orphan_files(
    table => 'local.db.my_table',
    older_than => TIMESTAMP '2025-05-24 00:00:00'
  )
""")
```

Note: Always ensure you have a backup before running maintenance operations, and verify the `older_than` timestamp carefully to avoid removing data that might still be needed.

## Catalog Support

| Catalog Type   | Status | Description                                    |
| -------------- | ------ | ---------------------------------------------- |
| Local Catalog  | ✅     | Hadoop-based local filesystem catalog          |
| AWS S3 Catalog | 🔄     | Store tables in S3 buckets (Coming soon)       |
| REST Catalog   | 🔄     | Use Iceberg REST catalog service (Coming soon) |

### DataFrame Operations

Create and write a DataFrame to an Iceberg table:

```elixir
iex> ExIcebergPort.dummy_df
{:ok,
 %ExIcebergPort.Result{
   columns: ["id", "name", "age"],
   rows: [],
   num_rows: 2,
   exec_time_ms: 207
 }}
```

### SQL Queries

Query data from Iceberg tables using SQL:

```elixir
iex> ExIcebergPort.query("select * from local.db.my_table")
{:ok,
 %ExIcebergPort.Result{
   sql: "select * from local.db.my_table",
   columns: ["id", "name", "age"],
   rows: [[1, "John_529", 18], [2, "Jane_595", 81]],
   num_rows: 2,
   exec_time_ms: 84
 }}
```

The result includes:

- `columns`: List of column names
- `rows`: List of data rows
- `num_rows`: Number of rows returned
- `exec_time_ms`: Query execution time in milliseconds
- `sql`: The SQL query that was executed (for SQL queries only)
