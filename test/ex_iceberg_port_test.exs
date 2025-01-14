defmodule ExIcebergPortTest do
  use ExUnit.Case
  doctest ExIcebergPort

  test "greets the world" do
    assert ExIcebergPort.hello() == :world
  end
end
