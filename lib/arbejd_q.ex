defmodule ArbejdQ do
  @moduledoc """
  Documentation for ArbejdQ.
  """

  alias ArbejdQ.Job

  @doc """
  Enqueue a new job.

  The job is enqueud in the given `queue`, and calls `worker_module` with
  the specified `parameters`.
  `parameters` is validated using `worker_module` and `{:error, :invalid_params}`
  is returned if the validation fails.
  """
  @spec enqueue_job(String.t, atom, term)
  :: {:ok, Job.t} | {:error, Ecto.Changeset.t} | {:error, :invalid_params}
  def enqueue_job(queue, worker_module, parameters) do
    with {:ok, _params} <- worker_module.validate_params(parameters)
    do
      Job.build(queue, worker_module, parameters,
                %{
                  status: :queued
                })
      |> repo().insert()
    else
      :error -> {:error, :invalid_params}
    end
  end

  @doc """
  Execute a queued job.

  This function is called by the scheduler, and should normally not be invoked
  manually.
  """
  @spec execute_job(Job.t) :: {:ok, Job.t} | {:error, :invalid_status}
  def execute_job(job) do
    try do
      # Reserve job by setting :status to :running
      # If someone else tried to run it, the optimistic locking scheme
      # will fail
      {:ok, job} =
        Job.changeset(job, %{status: :running, status_updated: DateTime.utc_now})
        |> repo().update
      result = job.worker_module.run(job.id, job.parameters)

      {:ok, job} = get_job(job.id)

      {:ok, job} =
        Job.changeset(job,
                      %{
                        status: :done,
                        status_updated: DateTime.utc_now,
                        result: result,
                      })
        |> repo().update

      {:ok, job}
    rescue
      Ecto.StaleEntryError -> {:error, :invalid_status}
    end
  end

  @doc """
  List all queued jobs within a queue.
  """
  @spec list_queued_jobs(String.t) :: [Job.t]
  def list_queued_jobs(queue) do
    Job.list_queued_jobs(queue)
    |> repo().all()
  end

  @doc """
  Retrieve a job given its `job_id`.
  """
  @spec get_job(String.t) :: {:ok, Job.t} | {:error, :not_found}
  def get_job(job_id) do
    res = Job.get_job(job_id)
          |> repo().all()
    case res do
      [] -> {:error, :not_found}
      [job] -> {:ok, job}
    end
  end

  @doc """
  Waits for job with `job_id` to complete.

  `timeout` specifies the maximum time to wait, before timing out and returning
  `{:error, :timeout}`.

  The job scan period is controlled by the `:job_scan_interval` configuration option.

  Returns `{:ok, result}`, where `result` is the result of the job.
  """
  @spec wait(String.t, non_neg_integer | :infinity) :: {:ok, any} | {:error, :timeout} | {:error, :not_found}
  def wait(job_id, timeout \\ :infinity) do
    with {:ok, job} <- get_job(job_id)
    do
      case job.status do
        :done -> {:ok, job.result}
        _ ->
          Process.sleep(1_000)
          new_timeout =
            case timeout do
              :infinity -> :infinity
              number -> number - 1_000
            end
          if new_timeout > 0 do
            wait(job_id, new_timeout)
          else
            :infinity
          end
      end
    else
      {:error, :not_found} ->
        {:error, :not_found}
    end
  end

  @spec list_stale_jobs(String.t) :: [Job.t]
  def list_stale_jobs(queue) do
    stale_period = stale_job_period()
    Job.list_stale_jobs(queue, Timex.subtract(Timex.now, Timex.Duration.from_seconds(stale_period)))
    |> repo().all
  end

  @doc """
  Get the stale job period in seconds.

  Configured as the `:stale_job_period` option of `:arbejd_q`.
  Default value is 60 seconds.
  """
  @spec stale_job_period :: non_neg_integer
  def stale_job_period do
    Application.get_env(:arbejd_q, :stale_job_period, 60)
  end

  @doc false
  def repo do
    Application.get_env(:arbejd_q, :repo)
  end
end
