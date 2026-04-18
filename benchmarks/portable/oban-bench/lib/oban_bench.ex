defmodule ObanBench do
  @moduledoc """
  Portable benchmark runner for Oban.
  Outputs JSON results to stdout, logs to stderr.
  """
  import Ecto.Query

  def run do
    scenario = System.get_env("SCENARIO", "all")
    job_count = env_int("JOB_COUNT", 10_000)
    worker_count = env_int("WORKER_COUNT", 50)
    latency_iterations = env_int("LATENCY_ITERATIONS", 100)

    case scenario do
      "migrate_only" ->
        # The application supervisor already ran migrations via ecto.setup.
        # Nothing else to do — just exit cleanly.
        IO.puts(:stderr, "[oban] migrate_only: migrations applied, exiting.")

      "worker_only" ->
        # Scale the chaos queue to the requested concurrency and block forever.
        # Jobs are expected to already exist in the database.
        chaos_concurrency = env_int("WORKER_COUNT", 10)
        Oban.scale_queue(queue: :chaos, limit: chaos_concurrency)
        IO.puts(:stderr, "[oban] worker_only: processing chaos queue (limit=#{chaos_concurrency}), blocking forever...")
        Process.sleep(:infinity)

      "long_horizon" ->
        ObanBench.LongHorizon.run()

      _ ->
        # Original benchmark scenarios
        run_benchmarks(scenario, job_count, worker_count, latency_iterations)
    end
  end

  defp run_benchmarks(scenario, job_count, worker_count, latency_iterations) do
    # Reconfigure Oban queues dynamically
    Oban.scale_queue(queue: :benchmark, limit: worker_count)

    results = []

    results =
      if scenario in ["all", "enqueue_throughput"] do
        IO.puts(:stderr, "[oban] Running enqueue_throughput...")
        results ++ [scenario_enqueue_throughput(job_count)]
      else
        results
      end

    results =
      if scenario in ["all", "worker_throughput"] do
        IO.puts(:stderr, "[oban] Running worker_throughput...")
        results ++ [scenario_worker_throughput(job_count, worker_count)]
      else
        results
      end

    results =
      if scenario in ["all", "pickup_latency"] do
        IO.puts(:stderr, "[oban] Running pickup_latency...")
        results ++ [scenario_pickup_latency(latency_iterations, worker_count)]
      else
        results
      end

    IO.puts(Jason.encode!(results, pretty: true))
  end

  # ── Scenarios ─────────────────────────────────────────────────

  defp scenario_enqueue_throughput(job_count) do
    clean_jobs()

    # Pause queue so jobs don't get consumed during insert
    Oban.pause_queue(queue: :benchmark)

    start = System.monotonic_time(:millisecond)

    # Insert in batches of 500
    1..job_count
    |> Enum.chunk_every(500)
    |> Enum.each(fn chunk ->
      changesets = Enum.map(chunk, &ObanBench.Worker.new(%{seq: &1}))
      Oban.insert_all(changesets)
    end)

    elapsed_ms = System.monotonic_time(:millisecond) - start
    jobs_per_sec = job_count / max(elapsed_ms / 1000.0, 0.001)

    Oban.resume_queue(queue: :benchmark)
    clean_jobs()

    %{
      system: "oban",
      scenario: "enqueue_throughput",
      config: %{job_count: job_count},
      results: %{
        duration_ms: elapsed_ms,
        jobs_per_sec: jobs_per_sec
      }
    }
  end

  defp scenario_worker_throughput(job_count, worker_count) do
    clean_jobs()

    # Pause queue, insert all jobs, then resume and measure drain
    Oban.pause_queue(queue: :benchmark)
    Process.sleep(200)

    1..job_count
    |> Enum.chunk_every(500)
    |> Enum.each(fn chunk ->
      changesets = Enum.map(chunk, &ObanBench.Worker.new(%{seq: &1}))
      Oban.insert_all(changesets)
    end)

    Oban.scale_queue(queue: :benchmark, limit: worker_count)

    start = System.monotonic_time(:millisecond)
    Oban.resume_queue(queue: :benchmark)

    wait_for_completion(job_count, 120_000)
    elapsed_ms = System.monotonic_time(:millisecond) - start

    jobs_per_sec = job_count / max(elapsed_ms / 1000.0, 0.001)
    clean_jobs()

    %{
      system: "oban",
      scenario: "worker_throughput",
      config: %{job_count: job_count, worker_count: worker_count},
      results: %{
        duration_ms: elapsed_ms,
        jobs_per_sec: jobs_per_sec
      }
    }
  end

  defp scenario_pickup_latency(iterations, worker_count) do
    clean_jobs()
    Oban.scale_queue(queue: :benchmark, limit: worker_count)
    Process.sleep(500)

    latencies_us =
      Enum.map(1..iterations, fn i ->
        start = System.monotonic_time(:microsecond)
        {:ok, _job} = Oban.insert(ObanBench.Worker.new(%{seq: i}))
        wait_for_completion(i, 10_000)
        System.monotonic_time(:microsecond) - start
      end)

    sorted = Enum.sort(latencies_us)
    n = length(sorted)
    p50 = Enum.at(sorted, div(n, 2))
    p95 = Enum.at(sorted, trunc(n * 0.95))
    p99 = Enum.at(sorted, min(trunc(n * 0.99), n - 1))
    mean = Enum.sum(sorted) / n

    clean_jobs()

    %{
      system: "oban",
      scenario: "pickup_latency",
      config: %{iterations: iterations, worker_count: worker_count},
      results: %{
        mean_us: mean,
        p50_us: p50,
        p95_us: p95,
        p99_us: p99
      }
    }
  end

  # ── Helpers ───────────────────────────────────────────────────

  defp clean_jobs do
    ObanBench.Repo.query!("DELETE FROM oban_jobs", [])
  end

  defp wait_for_completion(expected, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_completion(expected, deadline)
  end

  defp do_wait_completion(expected, deadline) do
    completed =
      ObanBench.Repo.one(
        from(j in "oban_jobs",
          where: j.state == "completed" and j.queue == "benchmark",
          select: count(j.id)
        )
      )

    cond do
      completed >= expected ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        counts = count_by_state()
        raise "Timeout: #{completed}/#{expected} completed, states: #{inspect(counts)}"

      true ->
        Process.sleep(50)
        do_wait_completion(expected, deadline)
    end
  end

  defp count_by_state do
    ObanBench.Repo.all(
      from(j in "oban_jobs",
        group_by: j.state,
        select: {j.state, count(j.id)}
      )
    )
    |> Map.new()
  end

  defp env_int(key, default) do
    case System.get_env(key) do
      nil -> default
      val -> String.to_integer(val)
    end
  end
end
