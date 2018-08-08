defmodule ArbejdQ.Test.Repo.Migrations.AddTables do
  use Ecto.Migration

  def up do
    ArbejdQ.Migrations.V1.up
  end

  def down do
    ArbejdQ.Migrations.V1.down
  end
end
