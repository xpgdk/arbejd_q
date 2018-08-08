defmodule ArbejdQ.Migrations.V1 do
  @moduledoc """
  Database migrations used by ArbejdQ version 1.
  """

  import Ecto.Migration

  def up do
    _ = create table(:arbejdq_jobs, primary_key: false) do
      add :id, :uuid, primary_key: true
      add :queue, :string, size: 255
      add :worker_module, :text
      add :parameters, :binary
      add :result, :binary
      add :progress, :binary
      add :worker_pid, :binary
      add :status, :integer
      add :status_updated, :utc_datetime
      add :expiration_time, :utc_datetime
      add :completion_time, :utc_datetime
      add :lock_version, :integer

      timestamps()
    end

    _ = create index(:arbejdq_jobs, [:queue])
    create index(:arbejdq_jobs, [:status])
  end

  def down do
    drop_if_exists table(:arbejdq_jobs)
  end
end
