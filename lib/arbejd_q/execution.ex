defmodule ArbejdQ.Execution do
  @moduledoc false

  alias ArbejdQ.{Job, Resources}

  require Logger

  @doc """
  Take a specific job.

  If successful, the job is marked as `:running' in the database. In that case `{:ok, job}`
  is returned.

  If the job cannot be taken, we assume that it is taken by someone else, and `{:error, :taken}` is returned.
  """
  @spec take_job(Job.t()) :: {:ok, Job.t()} | {:error, :taken | :missing_resources}
  def take_job(job) do
    try do
      ArbejdQ.repo().transaction(fn ->
        Resources.acquire_resources(job)

        {:ok, job} =
          Job.changeset(job, %{status: :running, status_updated: DateTime.utc_now()})
          |> ArbejdQ.repo().update()

        job
      end)
    rescue
      Ecto.InvalidChangesetError ->
        {:error, :missing_resources}

      Ecto.StaleEntryError ->
        {:error, :taken}
    end
  end

  @spec execute_job(Job.t(), (Job.t() -> :ok) | nil) ::
          {:ok, Job.t(), term} | {:error, :not_found}
  def execute_job(%Job{status: :running, id: job_id} = job, error_callback \\ nil) do
    job = assign_worker_pid(job, self())
    parameters = ArbejdQ.get_job_parameters(job)

    Process.put(ArbejdQ.JobId, job_id)

    result = sync_execute(job.worker_module, job.id, parameters)

    case ArbejdQ.get_job(job.id) do
      {:ok, job} ->
        case result do
          {:ok, job_result} ->
            job = commit_result(job, job_result)

            {:ok, job, job_result}

          {:error, error} ->
            job = commit_failure(job, error, error_callback)

            {:ok, job, error}
        end

      {:error, :not_found} ->
        Logger.warn(
          "ArbejdQ worker #{inspect(self())}: Job #{job.id} not found when trying to commit result"
        )

        {:error, :not_found}
    end
  end

  @spec sync_execute(module(), String.t(), map()) :: {:ok, any()} | {:error, any()}
  def sync_execute(worker_module, job_id, parameters) do

    try do
      job_result = worker_module.run(job_id, parameters)
      {:ok, job_result}
    rescue
      error ->
        {:error, error}
    end

  end

  @spec assign_worker_pid(Job.t(), pid) :: Job.t()
  defp assign_worker_pid(job, pid) do
    {:ok, job} =
      Job.changeset(job, %{worker_pid: pid})
      |> ArbejdQ.repo().update()

    job
  rescue
    Ecto.StaleEntryError ->
      {:ok, job} = ArbejdQ.get_job(job)
      assign_worker_pid(job, pid)
  end

  @spec commit_result(Job.t(), term) :: Job.t()
  def commit_result(%Job{status: :running} = job, result) do
    try do
      now = Timex.now()

      result =
        ArbejdQ.repo().transaction(fn ->
          params =
            %{
              status: :done,
              status_updated: now,
              completion_time: now,
              result: result
            }
            |> maybe_set_expiration_time(job)

          {:ok, job} =
            Job.changeset(job, params)
            |> ArbejdQ.repo().update

          Resources.free_resources(job)

          job
        end)

      case result do
        {:ok, job} -> job
      end
    rescue
      Ecto.StaleEntryError -> commit_result(ArbejdQ.repo().get(Job, job.id), result)
    end
  end

  def commit_result(job, _result) do
    job
  end

  @spec maybe_set_expiration_time(map, Job.t()) :: map
  defp maybe_set_expiration_time(params, %Job{expiration_time: nil}) do
    expiration_duration = ArbejdQ.default_expiration_duration()

    Map.put(
      params,
      :expiration_time,
      Timex.add(Timex.now(), Timex.Duration.from_seconds(expiration_duration))
    )
  end

  defp maybe_set_expiration_time(params, _job), do: params

  @spec commit_failure(Job.t(), any, (Job.t() -> :ok) | nil) :: Job.t()
  def commit_failure(%Job{status: status} = job, result, callback)
      when status in [:queued, :running] do
    try do
      now = Timex.now()

      {:ok, job} =
        ArbejdQ.repo().transaction(fn ->
          params =
            %{
              status: :failed,
              status_updated: now,
              completion_time: now,
              result: result
            }
            |> maybe_set_expiration_time(job)

          job =
            Job.changeset(job, params)
            |> ArbejdQ.repo().update!

          Resources.free_resources(job)

          if callback do
            callback.(job)
          end

          job
        end)

      job
    rescue
      Ecto.StaleEntryError -> commit_failure(job, result, callback)
    end
  end

  def commit_failure(%Job{} = job, _result, _callback), do: job
end
