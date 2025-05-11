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
cd jvm
sbt clean assembly
cd ..
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

## Catalog Support

| Catalog Type   | Status | Description                                    |
| -------------- | ------ | ---------------------------------------------- |
| Local Catalog  | ✅     | Hadoop-based local filesystem catalog          |
| AWS S3 Catalog | 🔄     | Store tables in S3 buckets (Coming soon)       |
| REST Catalog   | 🔄     | Use Iceberg REST catalog service (Coming soon) |
