# ExIcebergPort

Naive Iceberg implementation for Elixir via Java

## Usage

### Start the Port
To start the port, use the following command:
```elixir
ExIcebergPort.Java.start_link()
```

### Insert Dummy Rows
To insert dummy rows, use the following command:
```elixir
ExIcebergPort.insert_dummy_rows(10_000)
```

### Run Custom Insert
To run a custom insert, use the following command:
```elixir
insert_example = %{
  "name" => "table1",
  "cols" => [
    %{"name" => "id", "type" => "int"},
    %{"name" => "name", "type" => "string"}
  ],
  "rows" => [
    [1, "a"],
    [2, "b"]
  ]
}
ExIcebergPort.run(insert_example)
```