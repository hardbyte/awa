defmodule ObanBench.Worker do
  use Oban.Worker, queue: :benchmark, max_attempts: 3

  @impl Oban.Worker
  def perform(%Oban.Job{}) do
    :ok
  end
end
