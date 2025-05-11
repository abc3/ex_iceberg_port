defmodule ExIcebergPort do
  @moduledoc false

  @port_module ExIcebergPort.Jvm

  defdelegate start_link(opts \\ %{}), to: @port_module
  defdelegate stop(), to: @port_module
  defdelegate query(sql), to: @port_module

  def catalogs(), do: query("SHOW CATALOGS;")
  def databases(), do: query("SHOW DATABASES;")

  def describe_table(catalog, database, table),
    do: query("DESCRIBE TABLE #{catalog}.#{database}.#{table};")

  def dummy_start() do
    opts = %{
      warehouse_path: "./dev-warehouse",
      catalog_name: "local",
      executors_count: 4
    }

    start_link(opts)
  end
end
