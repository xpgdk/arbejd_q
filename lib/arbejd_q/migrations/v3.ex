defmodule ArbejdQ.Migrations.V3 do
  @moduledoc """
  Database migrations used by ArbejdQ version 3.
  """

  import Ecto.Migration

  def up do
    create table(:arbejdq_resources, primary_key: false) do
      add :id, :string, size: 255, primary_key: true
      add :type, :integer, null: false
      add :total_count, :integer, null: false
      add :available_count, :integer, null: false

      timestamps(type: :utc_datetime_usec)
    end

    alter table(:arbejdq_jobs) do
      add :resource_requirements, :map
    end
  end

  def down do
    drop_if_exists table(:arbejdq_resources)
    alter table(:arbejdq_jobs) do
      remove :resource_requirements
    end
  end
end
