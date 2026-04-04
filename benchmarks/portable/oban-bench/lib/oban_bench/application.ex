defmodule ObanBench.Application do
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      ObanBench.Repo,
      {Oban, Application.fetch_env!(:oban_bench, Oban)}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: ObanBench.Supervisor)
  end
end
