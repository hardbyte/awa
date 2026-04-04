defmodule ObanBench.Repo.Migrations.AddOban do
  use Ecto.Migration

  def up, do: Oban.Migration.up()
  def down, do: Oban.Migration.down()
end
