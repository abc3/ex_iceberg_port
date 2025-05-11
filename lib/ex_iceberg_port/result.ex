defmodule ExIcebergPort.Result do
  @type t :: %__MODULE__{
          sql: String.t(),
          columns: [String.t()],
          rows: [any()],
          num_rows: integer,
          exec_time_ms: integer
        }

  defstruct [:sql, :columns, :rows, :num_rows, :exec_time_ms]
end
