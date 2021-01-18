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

  use TypedEctoSchema

  import Ecto.Changeset
  import Ecto.Query

  alias ArbejdQ.{
    Types.Term,
    Job
  }

  @primary_key {:id, Ecto.UUID, autogenerate: true}
  @foreign_key_type Ecto.UUID
  @timestamps_opts [type: :utc_datetime_usec]

  typed_schema "arbejdq_jobs" do
    field(:queue, :string)
    field(:worker_module, ArbejdQ.Types.Atom)
    field(:result, Term)
    field(:progress, Term)
    field(:worker_pid, Term)
    field(:status, ArbejdQ.Types.Status)
    field(:status_updated, :utc_datetime_usec)
    field(:expiration_time, :utc_datetime_usec)
    field(:completion_time, :utc_datetime_usec)
    field(:lock_version, :integer, default: 1)
    field(:stale_counter, :integer, default: 0)
    embeds_many(:resource_requirements, ArbejdQ.ResourceRequirement)

    timestamps()
  end

  @spec changeset(Job.t(), map) :: Ecto.Changeset.t()
  def changeset(struct, params) do
    struct
    |> cast(params, [
      :queue,
      :worker_module,
      :result,
      :progress,
      :worker_pid,
      :status,
      :status_updated,
      :expiration_time,
      :completion_time,
      :stale_counter
    ])
    |> validate_required([:queue, :worker_module])
    |> optimistic_lock(:lock_version)
  end

  @spec build(String.t(), atom, term, map) :: Ecto.Multi.t()
  def build(queue, worker_module, parameters, params \\ %{}) do
    Ecto.Multi.new()
    |> Ecto.Multi.insert(
      :insert,
      %__MODULE__{}
      |> changeset(
        Map.merge(
          %{
            queue: queue,
            worker_module: worker_module
          },
          params
        )
      )
      |> maybe_put_resource_requirements(params)
    )
    |> Ecto.Multi.run(:update, fn _repo, changes ->
      job = changes[:insert]

      Ecto.Query.from(job in "arbejdq_jobs", where: job.id == ^UUID.string_to_binary!(job.id))
      |> ArbejdQ.repo().update_all(set: [parameters: :erlang.term_to_binary(parameters)])

      {:ok, job}
    end)
  end

  @spec maybe_put_resource_requirements(Ecto.Changeset.t(), map()) :: Ecto.Changeset.t()
  defp maybe_put_resource_requirements(changeset, %{resource_requirements: resource_requirements}) do
    changeset
    |> put_embed(:resource_requirements, resource_requirements)
  end
  defp maybe_put_resource_requirements(changeset, _), do: changeset

  @spec list_queued_jobs(String.t()) :: %Ecto.Query{}
  def list_queued_jobs(queue_name) do
    from(job in Job,
      where: job.queue == ^queue_name and job.status == ^:queued,
      order_by: job.inserted_at
    )
  end

  @spec list_queued_jobs(String.t(), non_neg_integer) :: %Ecto.Query{}
  def list_queued_jobs(queue_name, max_jobs) do
    from(job in Job,
      where: job.queue == ^queue_name and job.status == ^:queued,
      order_by: job.inserted_at,
      limit: ^max_jobs
    )
  end

  @spec get_job(String.t()) :: %Ecto.Query{}
  def get_job(job_id) do
    from(job in Job,
      where: job.id == ^job_id
    )
  end

  @spec list_stale_jobs(String.t(), DateTime.t()) :: %Ecto.Query{}
  def list_stale_jobs(queue_name, stale_progress_timestamp) do
    from(job in Job,
      where: job.queue == ^queue_name and job.status == ^:running,
      where: job.status_updated < ^stale_progress_timestamp,
      order_by: job.inserted_at
    )
  end

  @spec list_expired_jobs(String.t(), DateTime.t()) :: %Ecto.Query{}
  def list_expired_jobs(queue_name, expiration_time) do
    from(job in Job,
      where: job.queue == ^queue_name and job.status == ^:done,
      where: job.expiration_time < ^expiration_time,
      order_by: job.inserted_at
    )
  end

  @spec list_all :: %Ecto.Query{}
  def list_all do
    from(job in Job,
      order_by: job.inserted_at
    )
  end
end
