defmodule ExIcebergPort.Error do
  @type t :: %__MODULE__{
          sql: String.t(),
          message: String.t(),
          exec_time_ms: integer
        }

  defstruct [:sql, :message, :exec_time_ms]
end
