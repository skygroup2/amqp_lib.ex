defmodule AMQPEx.Worker do
  @moduledoc false
  use GenServer
  require Logger
  alias AMQPEx.Router
  import Skn.Util, only: [
    reset_timer: 3,
    cancel_timer: 2
  ]

  defmacro send_check(name, do: expression) do
    # will loss message in these case
    quote do
      try do
        ret = unquote(expression)
        case ret do
          :ok -> []
          :closing ->
            Logger.error("#{unquote(name)} send closing")
            {:next_event, :internal, :closed}
          # :blocked -> let this process crashed
        end
      catch
        _, exp ->
          Logger.error("#{unquote(name)} send exception #{inspect exp}")
          {:next_event, :internal, :closed}
      end
    end
  end

  def exchange_name(worker_name) do
    "ex_#{worker_name}"
  end

  def queue_name(worker_name, nil) do
    "q_#{worker_name}"
  end
  def queue_name(worker_name, rk) do
    "q_#{worker_name}_#{rk}"
  end

  def routing_key(rk) do
    case rk do
      nil -> ""
      :node -> Atom.to_string(node())
      _ -> rk
    end
  end

  def start_link(args) do
    :gen_statem.start_link({:local, args.name}, __MODULE__, args, [])
  end

  def init(args) do
    reset_timer(:reconnect, :reconnect, 5_000)
    reset_timer(:check_tick, :check_tick, 30_000)
    {:ok, :idle, %{
      name: args.name, type: args.type, recv_queue: [], send_queue: [],
      ex: exchange_name(args.name), q: queue_name(args.name, args.name_ext), rk: routing_key(args.rk),
      chan: nil, chan_pid: nil, chan_ref: nil, tag: nil,
      conn_name: args.conn_name, conn: nil, conn_pid: nil, conn_ref: nil, conn_get: false,
      misc: args.misc, tick_interval: Map.get(args.misc, :tick_interval, 10_000)
    }}
  end

  def idle(:info, :reconnect, %{name: name, conn_name: conn_name, conn_get: conn_get} = data) do
    if conn_get == false do
      AMQPEx.Connection.get(conn_name, name, self())
      reset_timer(:reconnect, :reconnect, 3000)
      {:keep_state, data}
    else
      {:keep_state, data}
    end
  end

  def idle(:info, {:connection_ack, conn_pid}, %{conn_pid: old_conn_pid} = data) do
    cancel_timer(:reconnect, :reconnect)
    if old_conn_pid != conn_pid do
      ref = Process.monitor(conn_pid)
      {:keep_state, %{data| conn_ref: ref, conn_pid: conn_pid}}
    else
      {:keep_state, data}
    end
  end

  def idle(:info, {:connection_report, conn}, %{name: name, type: type, q: q, ex: ex, rk: rk, misc: misc} = data) do
    # declare channel
    case AMQP.Channel.open(conn) do
      {:ok, channel} ->
        ref = Process.monitor(channel.pid)
        prefetch_count = Map.get(misc, :prefetch_count, 50)
        is_consumer = Map.get(misc, :is_consumer, true)
        ttl = Map.get(misc, :ttl, 120_000)
        AMQP.Basic.qos(channel, prefetch_count: prefetch_count)
        AMQP.Queue.declare(channel, q, durable: true, arguments: [{"x-message-ttl", ttl}])
        case type do
          :topic ->
            :ok = AMQP.Exchange.topic(channel, ex, durable: true)
            :ok = AMQP.Queue.bind(channel, q, ex, routing_key: rk)
          :direct ->
            :ok = AMQP.Exchange.direct(channel, ex, durable: true)
            :ok = AMQP.Queue.bind(channel, q, ex, routing_key: rk)
          :fanout ->
            :ok = AMQP.Exchange.fanout(channel, ex, durable: true)
            :ok = AMQP.Queue.bind(channel, q, ex)
        end
        if is_consumer == true do
          {:ok, tag} = AMQP.Basic.consume(channel, q)
          {:keep_state, %{data | chan: channel, chan_ref: ref, chan_pid: channel.pid, tag: tag}}
        else
          send(self(), :ready_no_consume)
          {:keep_state, %{data | chan: channel, chan_ref: ref, chan_pid: channel.pid, tag: nil}}
        end
      {:error, reason} ->
        Logger.error("#{name} open channel #{inspect reason}")
        {:stop, :normal, data}
    end
  end

  def idle(:info, {:basic_consume_ok, %{consumer_tag: _tag}}, %{name: name} = data) do
    Logger.debug("#{name} ready with consume")
    flush_send_queue(data)
  end

  def idle(:info, :ready_no_consume, %{name: name} = data) do
    Logger.debug("#{name} ready no consume")
    flush_send_queue(data)
  end

  def idle(:info, {:publish, msg, rk, opt}, %{send_queue: send_queue} = data) do
    send_queue = [{msg, rk, opt}| send_queue]
    {:keep_state, %{data| send_queue: send_queue}}
  end

  def idle(:info, {:flush, from, fun}, data) do
    process_flush_recv(from, fun, data)
  end

  def idle(:info, {:select, from, fun, default}, data) do
    process_select_recv(from, fun, default, data)
  end

  def idle(:info, :check_tick, %{name: name, recv_queue: recv_queue, tick_interval: tick_interval} = data) do
    recv_queue = remove_expiration(name, recv_queue, [])
    reset_timer(:check_tick, :check_tick, tick_interval)
    {:keep_state, %{data | recv_queue: recv_queue}}
  end

  def idle(:info, {:DOWN, _, :process, pid, reason}, %{name: name, conn_pid: pid} = data) do
    Logger.error("#{name} connection dead #{inspect reason}")
    reset_timer(:reconnect, :reconnect, 3000)
    {:keep_state, %{data| conn: nil, conn_pid: nil, conn_ref: nil, conn_get: false}}
  end

  def idle(:info, :quit, %{name: name} = data) do
    Logger.error("#{name} quiting")
    {:stop, :normal, data}
  end

  def idle(ev_type, ev_data, %{name: name} = data) do
    Logger.error("#{name} drop #{ev_type}:#{inspect ev_data}")
    {:keep_state, data}
  end

  def flush_send_queue(%{chan: channel, ex: ex, send_queue: send_queue} = data) do
    Enum.each Enum.reverse(send_queue), fn {msg, rk, opt} ->
      :ok = AMQP.Basic.publish(channel, ex, rk, msg, opt)
    end
    {:next_state, :ready, %{data | send_queue: []}}
  end

  def ready(:info, {:basic_deliver, payload, header}, %{name: name, chan: channel, recv_queue: recv_queue} = data) do
    h = format_expiration(header)
    m = payload_decode(payload, h)
    mid = h[:message_id]
    expiration = is_expiration?(h)
    next_event = send_check(name) do
      AMQP.Basic.ack(channel, h[:delivery_tag], [])
    end
    if expiration == false do
      pid = Router.get(mid)
      cond do
        is_pid(pid) and Process.alive?(pid) == true ->
          send(pid, {:AMQP, h, m})
          {:next_state, :ready, data, next_event}
        is_binary(mid) == true ->
          {:next_state, :ready, %{data| recv_queue: [{h, m}| recv_queue]}, next_event}
        true ->
          Logger.error("#{name} drop by NO_ROUTE #{inspect h}")
          {:next_state, :ready, data, next_event}
      end
    else
      Logger.error "#{name} drop by EXPIRED #{inspect h}"
      {:next_state, :ready, data, next_event}
    end
  end

  def ready(:info, {:publish, msg, rk, opt}, %{name: name, chan: channel, ex: ex} = data) do
    next_event = send_check(name) do
      AMQP.Basic.publish(channel, ex, rk, msg, opt)
    end
    {:next_state, :ready, data, next_event}
  end

  def ready(:info, {:flush, from, fun}, data) do
    process_flush_recv(from, fun, data)
  end

  def ready(:info, {:select, from, fun, default}, %{name: name, recv_queue: recv_queue} = data) do
    {h, m, recv_queue} = select_msg(name, recv_queue, fun, default, [])
    send(from, {:select_ack, self(), h, m})
    {:keep_state, %{data | recv_queue: recv_queue}}
  end

  def ready(:internal, :closed, data) do
    {:next_state, :idle, %{data| conn: nil}, {:next_event, :info, :reconnect}}
  end

  def ready(:info, :check_tick, %{name: name, recv_queue: recv_queue} = data) do
    recv_queue = remove_expiration(name, recv_queue, [])
    reset_timer(:check_tick, :check_tick, 25000)
    ready(:info, {:flush, nil, nil}, %{data | recv_queue: recv_queue})
  end

  def ready(:info, {:DOWN, _, :process, pid, reason}, %{name: name, conn_pid: pid} = data) do
    Logger.error("#{name} connection dead #{inspect reason}")
    reset_timer(:reconnect, :reconnect, 3000)
    {:next_state, :idle, %{data| conn: nil, conn_pid: nil, conn_ref: nil, conn_get: false}}
  end

  def ready(:info, {:DOWN, _, :process, pid, reason}, %{name: name, chan_pid: pid} = data) do
    Logger.error("#{name} channel dead #{inspect reason}")
    reset_timer(:reconnect, :reconnect, 3000)
    {:next_state, :idle, %{data| chan: nil, chan_pid: nil, chan_ref: nil, conn_get: false}}
  end

  def ready(:info, :quit, %{name: name} = data) do
    Logger.error("#{name} quiting")
    {:stop, :normal, data}
  end

  def ready(ev_type, ev_data, %{name: name} = data) do
    Logger.error("#{name} drop #{ev_type}:#{inspect ev_data}")
    {:keep_state, data}
  end

  def process_flush_recv(from, fun, %{name: name, recv_queue: recv_queue} = data) when is_function(fun) do
    {ret, recv_queue} = flush_msg(recv_queue, name, fun, [], [])
    Enum.each(ret, fn {h, m} -> send(from, {:AMQP, h, m}) end)
    {:keep_state, %{data| recv_queue: recv_queue}}
  end
  def process_flush_recv(_from, nil, %{name: name, recv_queue: recv_queue} = data) do
    recv_queue = Enum.reduce(Enum.reverse(recv_queue), [], fn {h, m}, acc ->
      if is_expiration?(h) == false do
        mid = h[:message_id]
        pid = Router.get(mid)
        if is_pid(pid) and Process.alive?(pid) == true do
          send(pid, {:AMQP, h, m})
          acc
        else
          [{h, m}| acc]
        end
      else
        Logger.error "#{name} drop by EXPIRED #{inspect h}"
        acc
      end
    end)
    {:keep_state, %{data| recv_queue: recv_queue}}
  end

  def process_select_recv(from, fun, default, %{name: name, recv_queue: recv_queue} = data) do
    {h, m, recv_queue} = select_msg(name, recv_queue, fun, default, [])
    send(from, {:select_ack, self(), h, m})
    {:keep_state, %{data | recv_queue: recv_queue}}
  end

  def callback_mode() do
    :state_functions
  end

  def terminate(reason, state_name, %{name: name, chan: channel, conn_pid: conn_pid}) do
    Logger.error "#{name} dead in #{state_name} by #{inspect reason}"
    if channel != nil do
      AMQP.Channel.close(channel)
    end
    if is_pid(conn_pid) do
      AMQPEx.Connection.del(conn_pid, name)
    end
    :ok
  end

  defp format_expiration(h) do
    expiration = h[:expiration]
    cond do
      is_integer(expiration) ->
        h
      is_binary(expiration) ->
        %{h| expiration: String.to_integer(expiration)}
      true ->
        %{h| expiration: System.system_time(:second) + 120}
    end
  end

  def is_expiration?(h) do
    ts_now = System.system_time(:second)
    ts_expiration = h[:expiration]
    ts_now > ts_expiration
  end

  def remove_expiration(_name, [], acc) do
    acc
  end

  def remove_expiration(name, [{h, m} | ret], acc) do
    expiration = is_expiration?(h)
    if expiration == false do
      remove_expiration(name, ret, [{h, m} | acc])
    else
      Logger.error "#{name} remove EXPIRED #{inspect h}"
      remove_expiration(name, ret, acc)
    end
  end

  def flush_msg([], _name, _fun, ret, acc) do
    {ret, Enum.reverse(acc)}
  end
  def flush_msg([{h, m}| remain], name, fun, ret, acc) do
    if is_expiration?(h) == false do
      if fun.(h, m) == true do
        flush_msg(remain, name, fun, [{h, m}| ret], acc)
      else
        flush_msg(remain, name, fun, ret, [{h, m}| acc])
      end
    else
      Logger.error "#{name} remove EXPIRED #{inspect h}"
      flush_msg(remain, name, fun, ret, acc)
    end
  end

  def select_msg(_name, [], _fun, default, acc) do
    {false, default, acc}
  end

  def select_msg(name, [{h, m} | ret], fun, default, acc) do
    expiration = is_expiration?(h)
    if expiration == false do
      cond do
        fun == nil ->
          {h, m, acc ++ ret}
        fun.(h, m) == true ->
          {h, m, acc ++ ret}
        true ->
          select_msg(name, ret, fun, default, [{h, m} | acc])
      end
    else
      # remove expired message
      Logger.error "#{name} remove EXPIRED #{inspect h}"
      select_msg(name, ret, fun, default, acc)
    end
  end

  def check_close(ret) do
    case ret do
      :ok ->
        []
      :blocked ->
        {:next_event, :internal, :closed}
      :closing ->
        {:next_event, :internal, :closed}
    end
  end

  def payload_encode(msg, opt) do
    m = if opt[:content_type] == "erl", do: :erlang.term_to_binary(msg), else: msg
    if opt[:content_encoding] == "gzip" do
      :zlib.zip(m)
    else
      m
    end
  end

  def payload_decode(msg, opt) do
    m = if opt[:content_encoding] == "gzip" do
      :zlib.unzip(msg)
    else
      msg
    end
    if opt[:content_type] == "erl", do: :erlang.binary_to_term(m), else: m
  end
end

defmodule AMQPEx.Connection do
  @moduledoc """
    handle and maintain connection
  """
  use GenServer
  require Logger
  require Skn.Log
  import Skn.Util, only: [
    reset_timer: 3
  ]

  def get(conn, worker_name, worker_pid) do
    GenServer.cast(conn, {:get, worker_name, worker_pid})
  end

  def del(conn, worker_name) do
    GenServer.cast(conn, worker_name)
  end

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: args.name)
  end

  def init(%{name: name, uri: uri}) do
    reset_timer(:reconnect, :reconnect, 5_000)
    {:ok, %{state: :connecting, pid: nil, ref: nil, conn: nil, channels: [], name: name, uri: uri}}
  end

  def handle_call(_msg, _from, data) do
    {:reply, :badarg, data}
  end

  def handle_cast({:get, worker_name, worker_pid}, %{state: state, conn: conn, channels: channels} = data) do
    if state == :connected do
      send(worker_pid, {:connection_ack, conn.pid})
      send(worker_pid, {:connection_report, conn})
    end
    channels = case List.keyfind(channels, worker_name, 0) do
      nil ->
        ref = Process.monitor(worker_pid)
        List.keystore(channels, worker_name, 0, {worker_name, worker_pid, ref})
      _ ->
        channels
    end
    {:noreply, %{data| channels: channels}}
  end

  def handle_cast(msg, data) do
    Skn.Log.error("drop cast #{inspect msg}")
    {:noreply, data}
  end

  def handle_info(:reconnect, %{uri: uri, channels: channels} = data) do
    case AMQP.Connection.open(uri) do
      {:ok, conn} ->
        ref = Process.monitor(conn.pid)
        Enum.each(channels, fn {_, x, _} ->
          send(x, {:connection_ack, conn.pid})
          send(x, {:connection_report, conn})
        end)
        {:noreply, %{data| pid: conn.pid, ref: ref, conn: conn, state: :connected}}
      {:error, reason} ->
        Skn.Log.error("connect error #{inspect reason} => retry")
        reset_timer(:reconnect, :reconnect, get_retry_delay(reason))
        {:noreply, data}
    end
  end

  def handle_info({:DOWN, _, :process, pid, reason}, %{pid: pid} = data) do
    Skn.Log.error("connection #{inspect pid} dead #{inspect reason}")
    reset_timer(:reconnect, :reconnect, 1_000)
    {:noreply, %{data| state: :connecting, pid: nil, ref: nil, conn: nil}}
  end

  def handle_info(msg, data) do
    Skn.Log.error("drop info #{inspect msg}")
    {:noreply, data}
  end

   def code_change(_vsn, state, _extra) do
    {:ok, state}
  end

  def terminate(reason, %{name: name}) do
    Skn.Log.error("connection #{name} dead #{inspect reason}")
    :ok
  end

  def get_retry_delay(:econnrefused), do: 20_000
  def get_retry_delay(_), do: 10_000
end
