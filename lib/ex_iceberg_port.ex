defmodule ExIcebergPort do
  @moduledoc false

  def run(map) do
    ExIcebergPort.Java.send(map)
  end

  def insert_dymmy_rows(number) do
    %{
      "name" => "table1",
      "cols" => [
        %{"name" => "id", "type" => "int"},
        %{"name" => "name", "type" => "string"}
      ],
      "rows" => Enum.map(1..number, fn x -> [x, "abc"] end)
    }
    |> run()
  end

  def run() do
    %{
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
    |> run()
  end
end
