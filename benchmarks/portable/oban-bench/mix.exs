defmodule ObanBench.MixProject do
  use Mix.Project

  def project do
    [
      app: :oban_bench,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: false,
      deps: deps(),
      aliases: aliases()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {ObanBench.Application, []}
    ]
  end

  defp deps do
    [
      {:oban, "~> 2.18"},
      {:postgrex, "~> 0.19"},
      {:ecto_sql, "~> 3.12"},
      {:jason, "~> 1.4"}
    ]
  end

  defp aliases do
    [
      "ecto.setup": ["ecto.create --quiet", "ecto.migrate --quiet"],
      "ecto.reset": ["ecto.drop --quiet", "ecto.setup"]
    ]
  end
end
