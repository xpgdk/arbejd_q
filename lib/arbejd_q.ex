defmodule ArbejdQ do
  @moduledoc """
  Documentation for ArbejdQ.
  """

  alias ArbejdQ.Execution
  alias ArbejdQ.Job
  alias ArbejdQ.ResourceRequirement
  alias ArbejdQ.Resource

  require Ecto.Query

  @type job_opt ::
          {:resources, [ResourceRequirement.t()]}
          | {:expiration_time, DateTime.t()}

  @type job_opts :: [job_opt]

  @doc """
  Enqueue a new job.

  The job is enqueud in the given `queue`, and calls `worker_module` with
  the specified `parameters`.
  `parameters` is validated using `worker_module` and `{:error, :invalid_params}`
  is returned if the validation fails.
  """
  @spec enqueue_job(String.t(), atom, term, job_opts) ::
          {:ok, Job.t()} | {:error, Ecto.Changeset.t()} | {:error, :invalid_params}
  def enqueue_job(queue, worker_module, parameters, opts \\ []) do
    with {:ok, _params} <- worker_module.validate_params(parameters),
         {:ok, job} <- create_job(queue, worker_module, parameters, opts) do
      scheduler_pid = Process.whereis(default_scheduler_name())

      if scheduler_pid != nil and Process.alive?(scheduler_pid) do
        ArbejdQ.Scheduler.poll_for_jobs(scheduler_pid)
      end

      {:ok, job}
    else
      :error -> {:error, :invalid_params}
      {:error, changeset} -> {:error, changeset}
    end
  end

  @spec enqueue_and_wait(String.t(), atom, term, job_opts(), timeout()) ::
          {:ok, :failed | :done, any}
          | {:error, :timeout}
          | {:error, :not_found}
          | {:error, Ecto.Changeset.t()}
          | {:error, :invalid_params}
  def enqueue_and_wait(queue, worker_module, parameters, opts, timeout \\ :infinity) do
    running_as_job_id = Process.get(ArbejdQ.JobId)

    if running_as_job_id != nil do
      case Execution.sync_execute(worker_module, running_as_job_id, parameters) do
        {:ok, result} ->
          {:ok, :done, result}

        {:error, error} ->
          {:ok, :failed, error}
      end
    else
      case enqueue_job(queue, worker_module, parameters, opts) do
        {:ok, %Job{} = job} ->
          wait(job, timeout)

        error ->
          error
      end
    end
  end

  @doc """
  List all resources in the system.
  """
  @spec list_resources() :: [Resource.t()]
  def list_resources() do
    repo().all(Resource)
  end

  @doc false
  @spec create_job(String.t(), atom, term, job_opts) ::
          {:ok, Job.t()} | {:error, Ecto.Changeset.t()}
  def create_job(queue, worker_module, parameters, opts \\ []) do
    case repo().transaction(
           Job.build(queue, worker_module, parameters, %{
             status: :queued,
             resource_requirements: Keyword.get(opts, :resources, []),
             expiration_time: Keyword.get(opts, :expiration_time)
           })
         ) do
      {:ok, changes} -> {:ok, changes[:update]}
      {:error, _, changeset, _} -> {:error, changeset}
    end
  end

  @spec get_job_parameters(Job.t() | String.t()) :: any
  def get_job_parameters(%Job{} = job) do
    get_job_parameters(job.id)
  end

  def get_job_parameters(job_id) when is_binary(job_id) do
    Ecto.Query.from(job in "arbejdq_jobs",
      where: job.id == ^Uniq.UUID.string_to_binary!(job_id),
      select: job.parameters
    )
    |> repo().one()
    |> :erlang.binary_to_term()
  end

  @doc """
  List all queued jobs within a queue.
  """
  @spec list_queued_jobs(String.t(), non_neg_integer) :: [Job.t()]
  def list_queued_jobs(queue, max_jobs) do
    Job.list_queued_jobs(queue, max_jobs)
    |> repo().all()
  end

  @doc """
  List all queued jobs within a queue.
  """
  @spec list_queued_jobs(String.t()) :: [Job.t()]
  def list_queued_jobs(queue) do
    Job.list_queued_jobs(queue)
    |> repo().all()
  end

  @doc """
  List all expired jobs within a queue.
  """
  @spec list_expired_jobs(String.t()) :: [Job.t()]
  def list_expired_jobs(queue) do
    Job.list_expired_jobs(queue, Timex.now())
    |> repo().all()
  end

  @doc """
  Retrieve a job given its `job_id`.
  """
  @spec get_job(String.t() | Job.t()) :: {:ok, Job.t()} | {:error, :not_found}
  def get_job(%Job{} = job) do
    get_job(job.id)
  end

  def get_job(job_id) do
    res =
      Job.get_job(job_id)
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
  @spec wait(String.t() | Job.t(), non_neg_integer | :infinity) ::
          {:ok, :failed | :done, any} | {:error, :timeout} | {:error, :not_found}
  def wait(job, timeout \\ :infinity)
  def wait(%Job{} = job, timeout), do: wait(job.id, timeout)

  def wait(job_id, timeout) do
    with {:ok, job} <- get_job(job_id) do
      case job.status do
        :done ->
          {:ok, :done, job.result}

        :failed ->
          {:ok, :failed, job.result}

        _ ->
          Process.sleep(1_000)

          new_timeout =
            case timeout do
              :infinity -> :infinity
              number -> number - 1_000
            end

          cond do
            new_timeout == :infinity ->
              wait(job_id, new_timeout)

            is_number(new_timeout) and new_timeout > 0 ->
              wait(job_id, new_timeout)

            is_number(new_timeout) ->
              {:error, :timeout}
          end
      end
    else
      {:error, :not_found} ->
        {:error, :not_found}
    end
  end

  @spec list_stale_jobs(String.t()) :: [Job.t()]
  def list_stale_jobs(queue) do
    stale_period = stale_job_period()

    queue
    |> Job.list_stale_jobs(Timex.subtract(Timex.now(), Timex.Duration.from_seconds(stale_period)))
    |> repo().all
  end

  @spec list_jobs(Job.job_list_opts()) :: [Job.t()]
  def list_jobs(job_list_opts \\ []) do
    job_list_opts
    |> Job.list()
    |> repo().all()
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

  @spec default_scheduler_name :: nil | atom
  def default_scheduler_name do
    Application.get_env(:arbejd_q, :default_scheduler_name, nil)
  end

  @doc """
  The the default expiration duration of completed jobs.

  The worker module is free to set the expiration time itself. If it does not,
  this default value is used.
  It is configured as the `:default_expiration_duration` option of `:arbejd_q`.
  The default is one hour (60 * 60 seconds).
  """
  @spec default_expiration_duration :: non_neg_integer
  def default_expiration_duration do
    Application.get_env(:arbejd_q, :default_expiration_duration, 60 * 60)
  end

  @doc false
  def repo do
    Application.get_env(:arbejd_q, :repo)
  end
end
