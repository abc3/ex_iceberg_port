defmodule ExIcebergPort do
  @moduledoc false

  @port_module ExIcebergPort.Jvm

  defdelegate start_link(opts \\ %{}), to: @port_module
  defdelegate stop(), to: @port_module
  defdelegate query(sql), to: @port_module
  defdelegate df(df), to: @port_module

  def catalogs(), do: query("SHOW CATALOGS;")
  def databases(), do: query("SHOW DATABASES;")

  def describe_table(catalog, database, table),
    do: query("DESCRIBE TABLE #{catalog}.#{database}.#{table};")

  def dummy_start() do
    opts = %{
      warehouse_path: "#{File.cwd!()}/dev-warehouse",
      catalog_name: "local",
      executors_count: 4
    }

    start_link(opts)
  end

  def dummy_df() do
    %{
      command: "dataframe",
      schema: [
        %{"column" => "id", "type" => "int8", "max_length" => nil, "null?" => false},
        %{"column" => "name", "type" => "varchar", "max_length" => 100, "null?" => true},
        %{"column" => "age", "type" => "int8", "max_length" => nil, "null?" => true}
      ],
      data: [
        ["1", "John_#{:rand.uniform(1000)}", "#{:rand.uniform(100)}"],
        ["2", "Jane_#{:rand.uniform(1000)}", "#{:rand.uniform(100)}"]
      ],
      table: "local.db.my_table"
    }
    |> df()
  end

  def dummy_df1() do
    %{
      command: "dataframe",
      schema: %{
        "id" => %{"type" => "int8", "max_length" => nil, "null?" => false},
        "name" => %{"type" => "varchar", "max_length" => 100, "null?" => true},
        "price" => %{"type" => "numeric", "max_length" => nil, "null?" => true},
        "created_at" => %{"type" => "timestamp", "max_length" => nil, "null?" => false},
        "is_active" => %{"type" => "bool", "max_length" => nil, "null?" => true},
        "metadata" => %{"type" => "jsonb", "max_length" => nil, "null?" => true}
      },
      data: [
        [1, "Product 1", 99.99, "2024-03-20T10:00:00", true, "{\"color\": \"red\"}"],
        [2, "Product 2", 149.99, "2024-03-20T11:00:00", false, "{\"color\": \"blue\"}"]
      ],
      table: "local.db.my_table"
    }
    |> df()
  end
end
