defmodule ExIcebergPort.Java do
  @moduledoc false
  use GenServer
  require Logger

  ## ------------------------------------------------------------------
  ## API Function Definitions
  ## ------------------------------------------------------------------

  def start_link(), do: start_link(%{})

  def start_link(opts), do: GenServer.start_link(__MODULE__, [opts], name: __MODULE__)

  def send(msg) when is_map(msg) do
    GenServer.call(__MODULE__, {:send, Jason.encode!(msg)})
  end

  ## ------------------------------------------------------------------
  ## gen_server Function Definitions
  ## ------------------------------------------------------------------

  def init(_) do
    Process.flag(:trap_exit, true)

    cmd = "mvn -f java/pom.xml compile exec:java -Dexec.mainClass='io.experiment.App'"

    Logger.info("Starting Java process with command: #{cmd}")

    port =
      Port.open({:spawn, cmd}, [
        :stderr_to_stdout,
        :use_stdio,
        :binary,
        :exit_status,
        {:line, 500}
      ])

    state = %{port: port, status: nil, caller: nil, called_at: nil}
    {:ok, state}
  end

  def handle_call({:send, msg}, from, %{status: :idle} = state) do
    true = Port.command(state.port, msg <> "\n")
    {:response, _response} = acc_resp(state.port)
    # {:reply, response, state}
    {:noreply, %{state | caller: from, status: :busy, called_at: ts()}}
  end

  def handle_info({_, {:data, {:eol, "wait for input"}}}, %{caller: nil} = state) do
    {:noreply, %{state | status: :idle}}
  end

  def handle_info({_, {:data, {:eol, "wait for input"}}}, state) do
    GenServer.reply(
      state.caller,
      {:ok, {:executed_ms, ts() - state.called_at}}
    )

    {:noreply, %{state | status: :idle, called_at: nil}}
  end

  def handle_info({_, {:data, {:eol, msg}}}, state) do
    case msg do
      "[WARNING]" <> _ -> Logger.warning("Java: #{msg}")
      "[ERROR]" <> _ -> Logger.error("Java: #{msg}")
      "[INFO] " -> nil
      _ -> Logger.info("Java: #{msg}")
    end

    {:noreply, state}
  end

  def handle_info(msg, state) do
    Logger.error("Unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end

  ## ------------------------------------------------------------------
  ## Internal Function Definitions
  ## ------------------------------------------------------------------

  defp acc_resp(port), do: acc_resp(port, "")

  defp acc_resp(_port, bin_acc) do
    receive do
      {_port, {:data, {:noeol, bin_data}}} ->
        acc_resp(Port, bin_acc <> bin_data)

      {_port, {:data, {:eol, bin_data}}} ->
        {:response, bin_acc <> bin_data}
    end
  end

  def ts(), do: System.monotonic_time(:millisecond)
end
