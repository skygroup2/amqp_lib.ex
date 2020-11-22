defmodule AMQPEx.Router do
  @moduledoc """
    Enable routing for non selective event
  """

  @table :AMQPEx_router
  def create_db() do
    case :ets.info(@table) do
      :undefined ->
        :ets.new(@table, [:public, :set, :named_table, {:read_concurrency, true}, {:write_concurrency, true}])
      _ ->
        :ok
    end
  end

  def gen_id() do
    :ets.update_counter(@table, :AMQPEx_router_seq, {2, 1, 4_294_967_295, 1}, {:AMQPEx_router_seq, 1})
  end

  def set(id, pid) do
    :ets.insert(@table, {id, pid})
  end

  def get(id) do
    case :ets.lookup(@table, id) do
      [{_id, pid}] -> pid
      _ -> nil
    end
  end

  def delete(id) do
    :ets.delete(@table, id)
  end
end
