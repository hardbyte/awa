defmodule ObanBench.Repo do
  use Ecto.Repo,
    otp_app: :oban_bench,
    adapter: Ecto.Adapters.Postgres
end
