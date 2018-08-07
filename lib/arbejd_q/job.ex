defmodule ArbejdQ.Job do
  @moduledoc """
  Job queued or running within ArbejdQ.

  When a job is `:running` it is taken by a worker node, and the job
  may not be executed by other nodes.

  `:status_updated` is to be updated at regular intervals. If `:status_update` has not
  been updated for too long, other nodes are free the execute the job.
  There are two configuration options controling these intervals:
  - `:update_interval` controls the interval between updating `:status_updated` (in seconds).
    Default: 60 (1 minute).
  - `:stale_job_period` is the period after which a job can be considered stale, and may be
    executed on a new worker node (in seconds).
    Default: 300 (5 minutes).
  """

  use Ecto.Schema
  use Timex.Ecto.Timestamps, usec: true

  import Ecto.Changeset
  import Ecto.Query

  alias ArbejdQ.{
    Types.Term,
    Job,
  }

  @primary_key {:id, Ecto.UUID, autogenerate: true}
  @foreign_key_type Ecto.UUID

  @type t :: %__MODULE__{}
  schema "arbejdq_jobs" do
    field :queue, :string
    field :worker_module, ArbejdQ.Types.Atom
    field :parameters, Term
    field :result, Term
    field :progress, Term
    field :status, ArbejdQ.Types.Status
    field :status_updated, :utc_datetime
    field :lock_version, :integer, default: 1

    timestamps()
  end

  @spec changeset(Job.t, map) :: Ecto.Changeset.t
  def changeset(struct, params) do
    struct
    |> cast(params, [:queue,
                     :worker_module,
                     :parameters,
                     :result,
                     :progress,
                     :status,
                     :status_updated])
    |> validate_required([:queue, :worker_module, :parameters])
    |> optimistic_lock(:lock_version)
  end

  @spec build(String.t, atom, term) :: Ecto.Changeset.t
  def build(queue, worker_module, job_parameters, params \\ %{}) do
    %__MODULE__{}
    |> changeset(Map.merge(
      %{
        queue: queue,
        worker_module: worker_module,
        parameters: job_parameters,
      },
      params))
  end

  @spec list_queued_jobs(String.t) :: %Ecto.Query{}
  def list_queued_jobs(queue_name) do
    from job in Job,
      where: job.queue == ^queue_name and job.status == ^:queued,
      order_by: job.inserted_at
  end

  @spec get_job(String.t) :: %Ecto.Query{}
  def get_job(job_id) do
    from job in Job,
      where: job.id == ^job_id
  end

  @spec list_stale_jobs(String.t, DateTime.t) :: %Ecto.Query{}
  def list_stale_jobs(queue_name, stale_progress_timestamp) do
    from job in Job,
      where: job.queue == ^queue_name and job.status == ^:running,
      where: job.status_updated < ^stale_progress_timestamp
  end
end