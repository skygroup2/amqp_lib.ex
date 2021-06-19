defmodule AMQPEx do
  @moduledoc """
    wrapper for AMQP
  """
  alias AMQPEx.Worker

  def publish(worker, msg, rk, opt) when is_map(opt) do
    publish(worker, msg, rk, Map.to_list(opt))
  end

  def publish(worker, msg, rk, opt) do
    send(worker, {:publish, Worker.payload_encode(msg, opt), rk, opt})
  end

  def select(worker, fun, default) do
    send(worker, {:select, self(), fun, default})
  end
end

defmodule AMQPEx.Sup do
  @moduledoc false
  use Supervisor
  @name :AMQPEx_sup

  def start_link(args) do
    Supervisor.start_link(__MODULE__, args, name: @name)
  end

  def init(_args) do
    children = [
    ]
    Supervisor.init(children, strategy: :one_for_one)
  end

  def start_connection(args) do
    spec = %{
      id: args.name,
      start: {AMQPEx.Connection, :start_link, [args]},
      restart: :transient,
      shutdown: 5000,
      type: :worker,
      modules: [AMQPEx.Connection]
    }
    Supervisor.start_child(@name, spec)
  end

  def start_worker(args) do
    spec = %{
      id: args.name,
      start: {AMQPEx.Worker, :start_link, [args]},
      restart: :permanent,
      shutdown: 5000,
      type: :worker,
      modules: [AMQPEx.Worker]
    }
    Supervisor.start_child(@name, spec)
  end
end
