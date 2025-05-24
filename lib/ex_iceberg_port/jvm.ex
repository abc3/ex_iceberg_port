defmodule ExIcebergPort.Jvm do
  @moduledoc false
  use GenServer
  require Logger

  alias ExIcebergPort.Result
  alias ExIcebergPort.Error

  ## ------------------------------------------------------------------
  ## API Function Definitions
  ## ------------------------------------------------------------------

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  def query(sql, timeout \\ :timer.seconds(25)),
    do: GenServer.call(__MODULE__, {:query, sql}, timeout)

  def df(df, timeout \\ :timer.seconds(25)),
    do: GenServer.call(__MODULE__, {:df, df}, timeout)

  def stop(timeout \\ :timer.seconds(25)), do: GenServer.stop(__MODULE__, timeout)

  ## ------------------------------------------------------------------
  ## gen_server Function Definitions
  ## ------------------------------------------------------------------

  @impl true
  def init(opts) do
    with {:ok, warehouse_path} <- validate_option(opts, :warehouse_path),
         _ <- File.mkdir_p(warehouse_path),
         {:ok, catalog_name} <- validate_option(opts, :catalog_name) do
      Process.flag(:trap_exit, true)

      Logger.info("Starting JVM process")

      state = %{
        port: nil,
        status: :busy,
        caller: nil,
        requested_at: 0,
        result: nil,
        sql: nil,
        query_mq: :queue.new(),
        jvm_logs?: opts[:logs?] || false,
        warehouse_path: warehouse_path,
        catalog_name: catalog_name,
        executors_count: opts[:executors_count] || 1
      }

      {:ok, state, {:continue, :start_port}}
    end
  end

  defp validate_option(opts, key) do
    case Map.fetch(opts, key) do
      {:ok, value} when not is_nil(value) -> {:ok, value}
      _ -> {:error, "Missing required option: #{key}"}
    end
  end

  @impl true
  def handle_continue(:start_port, state) do
    spark_configs = [
      # # Disable Spark UI
      # "-Dspark.ui.enabled=false",
      # # Force local mode with no network
      # "-Dspark.driver.host=local[1]",
      # "-Dspark.driver.bindAddress=127.0.0.1",
      # # Disable block transfer service
      # "-Dspark.block.manager.port=-1",
      # "-Dspark.driver.port=-1",
      # "-Dspark.port.maxRetries=0",
      # # Disable RPC
      # "-Dspark.rpc.io.mode=in-process",
      # # Minimize memory usage
      # "-Dspark.memory.offHeap.enabled=false"
      # # Minimize logging
      # # "-Dspark.log.level=ERROR"
    ]

    cmd =
      [
        "java",
        Enum.join(spark_configs, " "),
        "-jar",
        jvm_path() |> Path.join("target/scala-2.13/ice-assembly-0.1.0-SNAPSHOT.jar"),
        state.warehouse_path,
        state.catalog_name,
        to_string(state.executors_count)
      ]
      |> Enum.join(" ")

    Logger.info("Starting JVM process with command: #{cmd}")

    port =
      Port.open({:spawn, cmd}, [
        :stderr_to_stdout,
        :use_stdio,
        :binary,
        :exit_status
      ])

    {:noreply, %{state | port: port}}
  end

  @impl true
  def handle_call({:query, sql}, from, %{status: :idle} = state) do
    send_sql_command(state.port, sql)
    {:noreply, %{state | caller: from, status: :busy, requested_at: ts(), sql: sql}}
  end

  def handle_call({:df, df}, from, %{status: :idle} = state) do
    send_df_command(state.port, df)
    {:noreply, %{state | caller: from, status: :busy, requested_at: ts()}}
  end

  def handle_call({:query, sql}, from, %{status: :busy} = state) do
    req = {from, sql, ts()}
    Logger.debug("#{__MODULE__}: adding query to queue: #{inspect(req)}")
    {:noreply, %{state | query_mq: :queue.in(req, state.query_mq)}}
  end

  @impl true
  def handle_info({_, {:data, message}}, state) do
    state =
      case message do
        "WARNING: " <> message ->
          log(:warning, message, state.jvm_logs?)
          state

        "WARN: " <> message ->
          log(:warning, message, state.jvm_logs?)
          state

        "ERROR: " <> message ->
          log(:error, message, state.jvm_logs?)
          state

        "INFO: " <> message ->
          log(:info, message, state.jvm_logs?)
          state

        _ ->
          message
          |> Jason.decode()
          |> handle_response(state)
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

  defp handle_response(
         {_, %{"rows" => rows, "columns" => columns, "num_rows" => num_rows}},
         state
       ) do
    %{state | result: %Result{rows: rows, columns: columns, num_rows: num_rows}}
    |> do_response()
  end

  defp handle_response({_, %{"error" => error}}, state) do
    %{state | result: %Error{message: String.trim(error)}}
    |> do_response()
  end

  defp handle_response({_, %{"status" => "ready"}}, state), do: do_response(state)

  defp handle_response({type, %{data: data}}, state) do
    Logger.error("Unknown message: #{type} #{inspect(data)}")
    state
  end

  defp do_response(state) do
    if not is_nil(state.caller) and not is_nil(state.result) do
      response =
        state.result
        |> Map.put(:sql, state.sql)
        |> Map.put(:exec_time_ms, ts() - state.requested_at)
        |> wrap_result()

      GenServer.reply(state.caller, response)
    end

    case :queue.out(state.query_mq) do
      {:empty, _} ->
        %{state | status: :idle, result: nil}

      {{:value, {from, sql, ts}}, mq} ->
        true = send_sql_command(state.port, sql)
        %{state | query_mq: mq, caller: from, requested_at: ts, result: nil, sql: sql}
    end
  end

  defp jvm_path() do
    case System.get_env("MIX_ENV", "dev") do
      "dev" -> Path.join(File.cwd!(), "jvm")
      _ -> "#{:code.priv_dir(:ex_iceberg_port)}/jvm"
    end
  end

  @spec send_sql_command(port(), String.t()) :: boolean()
  defp send_sql_command(port, sql) do
    msg = Jason.encode!(%{command: "sql", sql: sql})
    Port.command(port, [msg, "\n"])
  end

  defp send_df_command(port, df) do
    msg =
      %{
        command: "dataframe",
        schema: df.schema,
        data: df.data,
        table: df.table
      }
      |> Jason.encode!()

    Port.command(port, [msg, "\n"])
  end

  @spec ts() :: non_neg_integer()
  def ts(), do: System.monotonic_time(:millisecond)

  defp wrap_result(%Result{} = result), do: {:ok, result}
  defp wrap_result(%Error{} = error), do: {:error, error}

  @spec log(atom(), String.t(), boolean()) :: :ok
  defp log(_, _, false), do: :ok

  defp log(level, message, _) do
    message = String.trim(message)
    apply(Logger, level, ["SPARK: #{message}"])
  end
end
