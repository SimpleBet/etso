defmodule Etso.Adapter.Behaviour.Queryable do
  @moduledoc false

  Module.register_attribute(__MODULE__, :telemetry_event,
        accumulate: true,
        persist: true
      )

  alias Etso.Adapter.TableRegistry
  alias Etso.ETS.MatchSpecification

  def prepare(:all, query) do
    {:nocache, query}
  end

  def execute(%{repo: repo}, _, {:nocache, query}, params, _) do
    start_time = System.monotonic_time()

    {_, schema} = query.from.source
    {:ok, ets_table} = TableRegistry.get_table(repo, schema)
    ets_match = MatchSpecification.build(query, params)
    start_metadata = %{
      type: :etso_query,
      repo: repo,
      params: params,
      query: ets_match,
      source: schema
    }

    try do
      :telemetry.execute([:ecto, :query, :start], %{system_time: System.system_time()}, start_metadata)
      :ets.select(ets_table, [ets_match])
    catch
      kind, reason ->
        stacktrace = __STACKTRACE__
        metadata = %{kind: kind, reason: reason, stacktrace: stacktrace}
        :telemetry.execute([:ecto, :query, :exception], %{duration: System.monotonic_time() - start_time}, metadata)
        :erlang.raise(kind, reason, stacktrace)
    else
      ets_objects ->
        object_count = length(ets_objects)
        stop_metadata = Map.put(start_metadata, :result, ets_objects)
        :telemetry.execute([:ecto, :query, :stop], %{duration: System.monotonic_time() - start_time}, stop_metadata)
        {object_count, ets_objects}
    end
  end

  def stream(%{repo: repo}, _, {:nocache, query}, params, options) do
    {_, schema} = query.from.source
    {:ok, ets_table} = TableRegistry.get_table(repo, schema)
    ets_match = MatchSpecification.build(query, params)
    ets_limit = Keyword.get(options, :max_rows, 500)
    stream_start_fun = fn -> stream_start(ets_table, ets_match, ets_limit) end
    stream_next_fun = fn acc -> stream_next(acc) end
    stream_after_fun = fn acc -> stream_after(ets_table, acc) end
    Stream.resource(stream_start_fun, stream_next_fun, stream_after_fun)
  end

  defp stream_start(ets_table, ets_match, ets_limit) do
    :ets.safe_fixtable(ets_table, true)
    :ets.select(ets_table, [ets_match], ets_limit)
  end

  defp stream_next(:"$end_of_table") do
    {:halt, :ok}
  end

  defp stream_next({ets_objects, ets_continuation}) do
    {[{length(ets_objects), ets_objects}], :ets.select(ets_continuation)}
  end

  defp stream_after(ets_table, :ok) do
    :ets.safe_fixtable(ets_table, false)
  end

  defp stream_after(_, acc) do
    acc
  end
end
