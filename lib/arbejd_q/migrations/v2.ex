defmodule ArbejdQ.Migrations.V2 do
  @moduledoc """
  Database migrations used by ArbejdQ version 2.
  """

  import Ecto.Migration

  def up do
    alter table(:arbejdq_jobs) do
      add :stale_counter, :integer, default: 0
    end
  end

  def down do
    alter table(:arbejdq_jobs) do
      remove :stale_counter
    end
  end
end
