defmodule ArbejdQ.Execution do
  @moduledoc false

  alias ArbejdQ.Job

  @doc """
  Take a specific job.

  If successful, the job is marked as `:running' in the database. In that case `{:ok, job}`
  is returned.

  If the job cannot be taken, we assume that it is taken by someone else, and `{:error, :taken}` is returned.
  """
  @spec take_job(Job.t) :: {:ok, Job.t} | {:error, :taken}
  def take_job(job) do
    try do
      {:ok, job} =
        Job.changeset(job, %{status: :running, status_updated: DateTime.utc_now})
        |> ArbejdQ.repo().update()

      {:ok, job}
    rescue
      Ecto.StaleEntryError ->
        {:error, :taken}
    end
  end

  @spec execute_job(Job.t) :: {:ok, Job.t, term}
  def execute_job(%Job{status: :running} = job) do
    job = assign_worker_pid(job, self())
    result = job.worker_module.run(job.id, job.parameters)
    {:ok, job} = ArbejdQ.get_job(job.id)
    job = commit_result(job, result)

    {:ok, job, result}
  end

  @spec assign_worker_pid(Job.t, pid) :: Job.t
  defp assign_worker_pid(job, pid) do
    {:ok, job} =
      Job.changeset(job, %{worker_pid: pid})
      |> ArbejdQ.repo().update()

    job
  end

  @spec commit_result(Job.t, term) :: Job.t
  def commit_result(%Job{status: :running} = job, result) do
    try do
      now = Timex.now
      params =
        %{
          status: :done,
          status_updated: now,
          completion_time: now,
          result: result,
        }
        |> maybe_set_expiration_time(job)

      {:ok, job} =
        Job.changeset(job, params)
        |> ArbejdQ.repo().update
      job
    rescue
      Ecto.StaleEntryError -> commit_result(ArbejdQ.repo().get(Job, job.id), result)
    end
  end
  def commit_result(job, _result) do
    job
  end


  @spec maybe_set_expiration_time(map, Job.t) :: map
  defp maybe_set_expiration_time(params, %Job{expiration_time: nil}) do
    expiration_duration = ArbejdQ.default_expiration_duration
    Map.put(params, :expiration_time, Timex.add(Timex.now, Timex.Duration.from_seconds(expiration_duration)))
  end
  defp maybe_set_expiration_time(params, _job), do: params

  @spec commit_failure(Job.t, any) :: Job.t
  def commit_failure(%Job{status: status} = job, result) when status in [:queued, :running] do
    try do
      now = Timex.now
      params =
        %{
          status: :failed,
          status_updated: now,
          completion_time: now,
          result: result,
        }
        |> maybe_set_expiration_time(job)

      {:ok, job} =
        Job.changeset(job, params)
        |> ArbejdQ.repo().update
      job
    rescue
      Ecto.StaleEntryError -> commit_failure(job, result)
    end
  end
  def commit_failure(%Job{} = job, _result), do: job
end
