defmodule ObanBench.ChaosWorker do
  @moduledoc """
  A long-running worker used by chaos test scenarios.
  Each job sleeps for JOB_DURATION_MS (default 30 000 ms) then completes.
  Configured with max_attempts: 5 so Lifeline rescues can be observed.
  """
  use Oban.Worker, queue: :chaos, max_attempts: 5

  @impl Oban.Worker
  def perform(%Oban.Job{}) do
    duration_ms =
      case System.get_env("JOB_DURATION_MS") do
        nil -> 30_000
        val -> String.to_integer(val)
      end

    Process.sleep(duration_ms)
    :ok
  end
end
