defmodule ObanBench.LongHorizon do
  @moduledoc """
  Long-horizon scenario for the portable bench.

  Fixed-rate producer + steady consumer with clock-aligned JSONL telemetry
  samples every SAMPLE_EVERY_S seconds. Contract:
  benchmarks/portable/CONTRIBUTING_ADAPTERS.md

  Lifecycle: emits one descriptor record, runs until the BEAM exits (the
  bench harness sends SIGTERM to the Docker container, which terminates the
  release; stdout is line-buffered so all emitted samples survive).
  """

  import Ecto.Query
  alias ObanBench.{Repo, LongHorizonWorker}

  @queue :long_horizon_bench
  @latency_window_s 30.0

  def run do
    sample_every_s = env_int("SAMPLE_EVERY_S", 10)
    producer_rate = env_int("PRODUCER_RATE", 800)
    worker_count = env_int("WORKER_COUNT", 32)
    work_ms = env_int("JOB_WORK_MS", 1)
    payload_bytes = env_int("JOB_PAYLOAD_BYTES", 256)

    db_name =
      System.get_env("DATABASE_URL", "")
      |> String.split("/")
      |> List.last()
      |> case do
        nil -> "oban_bench"
        "" -> "oban_bench"
        other -> other
      end

    emit(%{
      kind: "descriptor",
      system: "oban",
      event_tables: ["public.oban_jobs", "public.oban_peers"],
      extensions: [],
      version: "0.1.0",
      schema_version: System.get_env("OBAN_SCHEMA_VERSION", "current"),
      db_name: db_name,
      started_at: now_iso()
    })

    :ets.new(:long_horizon_state, [:public, :named_table, :set])
    :ets.insert(:long_horizon_state, {:enqueued, 0})
    :ets.insert(:long_horizon_state, {:completed, 0})
    :ets.insert(:long_horizon_state, {:work_ms, work_ms})
    :ets.insert(:long_horizon_state, {:queue_depth, 0})
    :ets.new(:long_horizon_lat, [:public, :named_table, :duplicate_bag])

    Oban.scale_queue(queue: @queue, limit: worker_count)

    padding = String.duplicate("x", max(0, payload_bytes - 32))

    _producer = spawn_link(fn -> producer_loop(producer_rate, padding) end)
    _sampler = spawn_link(fn -> sampler_loop(sample_every_s) end)
    _depth = spawn_link(fn -> depth_loop() end)

    # Block forever — the harness SIGTERMs the container.
    Process.sleep(:infinity)
  end

  defp producer_loop(rate, _padding) when rate <= 0, do: :ok

  defp producer_loop(rate, padding) do
    period_ms = max(1, div(1000, rate))
    producer_step(0, period_ms, padding)
  end

  defp producer_step(seq, period_ms, padding) do
    Process.sleep(period_ms)

    case Oban.insert(LongHorizonWorker.new(%{seq: seq, padding: padding})) do
      {:ok, _} -> :ets.update_counter(:long_horizon_state, :enqueued, 1)
      _ -> :ok
    end

    producer_step(seq + 1, period_ms, padding)
  end

  defp depth_loop do
    Process.sleep(1000)

    depth =
      case Repo.one(
             from(j in "oban_jobs",
               where: j.state == "available" and j.queue == "long_horizon_bench",
               select: count(j.id)
             )
           ) do
        n when is_integer(n) -> n
        _ -> 0
      end

    :ets.insert(:long_horizon_state, {:queue_depth, depth})
    depth_loop()
  end

  defp sampler_loop(sample_every_s) do
    # Align to wall-clock boundary.
    now_epoch = System.system_time(:second)
    sleep_for_s = sample_every_s - rem(now_epoch, sample_every_s)
    Process.sleep(sleep_for_s * 1000)
    sampler_step(0, 0, System.monotonic_time(:millisecond), sample_every_s)
  end

  defp sampler_step(last_enq, last_cmp, last_tick_ms, sample_every_s) do
    Process.sleep(sample_every_s * 1000)

    [{:enqueued, enq}] = :ets.lookup(:long_horizon_state, :enqueued)
    [{:completed, cmp}] = :ets.lookup(:long_horizon_state, :completed)
    [{:queue_depth, depth}] = :ets.lookup(:long_horizon_state, :queue_depth)
    now_ms = System.monotonic_time(:millisecond)
    dt_s = max(0.001, (now_ms - last_tick_ms) / 1000.0)
    enq_rate = (enq - last_enq) / dt_s
    cmp_rate = (cmp - last_cmp) / dt_s

    {p50, p95, p99} = percentiles_ms(@latency_window_s)
    ts = now_iso()

    Enum.each(
      [
        {"claim_p50_ms", p50, 30},
        {"claim_p95_ms", p95, 30},
        {"claim_p99_ms", p99, 30},
        {"enqueue_rate", enq_rate, sample_every_s},
        {"completion_rate", cmp_rate, sample_every_s},
        {"queue_depth", depth * 1.0, 0}
      ],
      fn {name, value, window_s} ->
        emit(%{
          t: ts,
          system: "oban",
          kind: "adapter",
          subject_kind: "adapter",
          subject: "",
          metric: name,
          value: value,
          window_s: window_s
        })
      end
    )

    sampler_step(enq, cmp, now_ms, sample_every_s)
  end

  defp percentiles_ms(window_s) do
    cutoff = System.monotonic_time(:millisecond) - trunc(window_s * 1000)

    values =
      :ets.tab2list(:long_horizon_lat)
      |> Enum.flat_map(fn {_, ts_ms, lat_ms} ->
        if ts_ms >= cutoff, do: [lat_ms], else: []
      end)

    case values do
      [] ->
        {0.0, 0.0, 0.0}

      _ ->
        sorted = Enum.sort(values)
        n = length(sorted)

        pick = fn q ->
          idx = min(n - 1, max(0, round(q * (n - 1))))
          Enum.at(sorted, idx) * 1.0
        end

        {pick.(0.50), pick.(0.95), pick.(0.99)}
    end
  end

  defp emit(record) do
    IO.puts(Jason.encode!(record))
  end

  defp now_iso do
    DateTime.utc_now()
    |> DateTime.to_iso8601()
    |> String.replace(~r/\+00:00$/, "Z")
  end

  defp env_int(key, default) do
    case System.get_env(key) do
      nil -> default
      val -> String.to_integer(val)
    end
  end
end

defmodule ObanBench.LongHorizonWorker do
  use Oban.Worker, queue: :long_horizon_bench, max_attempts: 3

  @impl Oban.Worker
  def perform(%Oban.Job{inserted_at: inserted_at}) do
    now = DateTime.utc_now()
    latency_ms = max(0, DateTime.diff(now, inserted_at, :millisecond))
    ts_ms = System.monotonic_time(:millisecond)

    :ets.insert(:long_horizon_lat, {:lat, ts_ms, latency_ms * 1.0})

    # Coarse trim: when the ETS bag gets too large, drop everything older than
    # the rolling window. Cheap enough at this frequency.
    if :rand.uniform(200) == 1 do
      cutoff = ts_ms - 30_000

      :ets.foldl(
        fn {_, t, _} = obj, acc ->
          if t < cutoff, do: [obj | acc], else: acc
        end,
        [],
        :long_horizon_lat
      )
      |> Enum.each(&:ets.delete_object(:long_horizon_lat, &1))
    end

    work_ms =
      case :ets.lookup(:long_horizon_state, :work_ms) do
        [{:work_ms, v}] -> v
        _ -> 1
      end

    if work_ms > 0, do: Process.sleep(work_ms)
    :ets.update_counter(:long_horizon_state, :completed, 1)
    :ok
  end
end
