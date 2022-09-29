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

  @type uuid :: binary()
  @type id :: uuid()

  @type worker_module :: ArbejdQ.Types.Atom.t()

  @type job_list_sort_direction :: :asc | :desc
  @type queue_name :: String.t()
  @type jobs_amount :: non_neg_integer()
  @type max_allowed_age_hrs :: non_neg_integer()

  @type job_list_opt ::
          {:include_parameters, boolean()}
          | {:include_result, boolean()}
          | {:jobs_to_show, jobs_amount()}
          | {:sort, job_list_sort_direction()}
          | {:queue, queue_name()}
          | {:statuses, list(ArbejdQ.Types.Status.t()) | ArbejdQ.Types.Status.t()}
          | {:updated_before, DateTime.t()}
          | {:updated_after, DateTime.t()}
          | {:expired_before, DateTime.t()}
          | {:worker_module, worker_module()}
          | {:exclude_finished_export_jobs, max_allowed_age_hrs()}
          | {:limit, integer()}

  @type job_list_opts :: [job_list_opt()]

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

    field(:parameters, Term, virtual: true)

    timestamps()
  end

  @fields_for_job_list ~w(
      id
      queue
      worker_module
      progress
      worker_pid
      status
      status_updated
      expiration_time
      completion_time
      lock_version
      stale_counter
      resource_requirements
      inserted_at
      updated_at
  )a

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

  @spec build(queue_name, worker_module, term, map) :: Ecto.Multi.t()
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
    put_embed(changeset, :resource_requirements, resource_requirements)
  end

  defp maybe_put_resource_requirements(changeset, _), do: changeset

  @spec get_job(id()) :: %Ecto.Query{}
  def get_job(job_id) do
    from(job in Job,
      where: job.id == ^job_id
    )
  end

  @spec list_queued_jobs(queue_name) :: %Ecto.Query{}
  def list_queued_jobs(queue_name) do
    [
      queue: queue_name,
      statuses: :queued,
      include_result: true
    ]
    |> list()
  end

  @spec list_queued_jobs(queue_name, jobs_amount) :: %Ecto.Query{}
  def list_queued_jobs(queue_name, max_jobs) do
    [
      queue: queue_name,
      statuses: :queued,
      limit: max_jobs
    ]
    |> list()
  end

  @spec list_stale_jobs(queue_name, DateTime.t()) :: %Ecto.Query{}
  def list_stale_jobs(queue_name, stale_progress_timestamp) do
    [
      queue: queue_name,
      updated_before: stale_progress_timestamp,
      statuses: :running
    ]
    |> list()
  end

  @spec list_expired_jobs(queue_name, DateTime.t()) :: %Ecto.Query{}
  def list_expired_jobs(queue_name, expiration_time) do
    [
      queue: queue_name,
      expired_before: expiration_time,
      statuses: :done
    ]
    |> list()
  end

  @spec list_all() :: %Ecto.Query{}
  def list_all, do: list([])

  @spec list(job_list_opts) :: %Ecto.Query{}
  def list(job_list_opts \\ []) do
    base_query = from(job in Job, as: :job, select: ^@fields_for_job_list)

    job_list_opts
    |> Keyword.put_new(:sort, :asc)
    |> Enum.reduce(base_query, &apply_opt/2)
  end

  defp apply_opt({:sort, sort_direction}, query) when sort_direction in [:asc, :desc] do
    order_by(query, [job: job], {^sort_direction, job.inserted_at})
  end

  defp apply_opt({:include_parameters, true}, query) do
    query
    |> join(:left, [job: job], job2 in "arbejdq_jobs", on: job.id == job2.id, as: :job2)
    |> select_merge([job2: job2], %{parameters: type(job2.parameters, Term)})
  end

  defp apply_opt({:include_result, true}, query) do
    select_merge(query, [job: job], ^[:result])
  end

  defp apply_opt({:statuses, statuses}, query) do
    where(query, [job: job], job.status in ^List.wrap(statuses))
  end

  defp apply_opt({:queue, queue_name}, query) do
    where(query, [job: job], job.queue == ^queue_name)
  end

  defp apply_opt({:updated_before, datetime}, query) do
    where(query, [job: job], job.status_updated < ^datetime)
  end

  defp apply_opt({:updated_after, datetime}, query) do
    where(query, [job: job], job.status_updated > ^datetime)
  end

  defp apply_opt({:exclude_finished_export_jobs, max_allowed_age_hrs}, query) do
    max_age_datetime = Timex.shift(Timex.now(), hours: -max_allowed_age_hrs)

    where(
      query,
      [job: job],
      not (job.worker_module == ^QarmaInspect.Exporter.V1 and
             job.status == :done and
             job.completion_time <= ^max_age_datetime)
    )
  end

  defp apply_opt({:expired_before, datetime}, query) do
    where(query, [job: job], job.expiration_time < ^datetime)
  end

  defp apply_opt({:jobs_to_show, amount}, query) do
    limit(query, ^amount)
  end

  # It might worth to refactor worker_module filters to work with strings instead of atoms:
  # in situations when worker module namespace removed from the system but it still has
  # previous jobs in DB.
  #
  # defp apply_opt({:worker_module, worker_module}, query) when is_binary(worker_module) do
  #   try do
  #     worker_module
  #     |> String.to_existing_atom()
  #     |> then(&{:worker_module, &1})
  #     |> apply_opt(query)
  #   rescue
  #     # If there is no necessary atom available then we can't
  #     _ -> query
  #   end
  # end

  defp apply_opt({:worker_module, worker_module}, query) when is_atom(worker_module) do
    where(query, [job: job], job.worker_module == ^worker_module)
  end

  defp apply_opt(_, query), do: query
end
