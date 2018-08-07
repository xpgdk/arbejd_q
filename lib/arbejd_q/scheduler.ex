defmodule ArbejdQ.Scheduler do
  @moduledoc """
  Responsible for scheduling jobs for execution.

  The scheduler is configured with a separate scheduling configuration for
  each queue, which is setup as an argument to `init/1`.
  """

  use GenServer

  alias ArbejdQ.Job

  @type queue_config :: %{
    max_jobs: non_neg_integer,
    priority: non_neg_integer,
  }

  @type opts :: [
    queues: %{
      required(atom) => queue_config
    },
    max_jobs: non_neg_integer,
    poll_interval: non_neg_integer,
  ]

  @typep worker :: %{
    pid: pid,
    queue: atom,
    job_id: String.t,
  }

  @typep state :: %{
    queues: %{
      required(atom) => queue_config
    },
    max_jobs: non_neg_integer,
    poll_interval: non_neg_integer,
    workers: [worker],
    last_time_of_poll: DateTime.t,
    last_time_of_job_refresh: DateTime.t,
    last_time_of_stale_reset: DateTime.t,
  }

  @doc """
  Start the scheduler.

  Argument is an keyword list with the following keys:
  - `:queues` defines the scheduling configuration for each queue.
  - `:max_jobs` total maximum jobs executed between all queues.
  - `:poll_interval` time (in seconds) between polling for new jobs to run.
    (default value is 30s).

  Queue configuration is done through the `:queues` option. Only jobs from
  explicitly configured queues are executed.  The option is a keyword list,
  with the queue name as key, and the value a keyword list itself, with the
  following options:
  - `:max_jobs` maximum number of jobs to execute from the queue.
  - `:priority` priority of the queue. Queue with the highest priority number are
    consideret first, when scheduling jobs.
  """
  @spec start_link(opts, GenServer.options) :: GenServer.on_start
  def start_link(opts, gen_server_opts \\ []) do
    GenServer.start_link(__MODULE__, opts, gen_server_opts)
  end

  @spec stop(pid) :: :ok
  def stop(pid) do
    GenServer.stop(pid)
  end

  def init(opts) do
    queues =
      Keyword.get(opts, :queues, [])
      |> Enum.sort_by(fn {_k, v} ->
        Keyword.get(v, :priority, 1)
      end, &>/2)
    max_jobs = Keyword.get(opts, :max_jobs, 0)
    poll_interval = Keyword.get(opts, :poll_interval, 30)

    initial_state = %{
      max_jobs: max_jobs,
      poll_interval: poll_interval,
      queues: queues,
      workers: [],
      last_time_of_poll: Timex.now,
      last_time_of_job_refresh: Timex.now,
      last_time_of_stale_reset: Timex.now,
    }

    restart_timer(initial_state)

    IO.puts ">>>>>>>> Scheduler starting with PID #{inspect self()}"
    {:ok, initial_state}
  end

  def terminate(reason, _state) do
    IO.puts "<<<<<<<< Scheduler with PID #{inspect self()} terminating with reason #{inspect reason}"
    :ok
  end

  def handle_cast({:job_done, _job_id}, state) do
    {:noreply, state}
  end

  def handle_info(:handle_timer, state) do
    state =
      state
      |> maybe_handle_poll_timeout
      |> maybe_handle_job_refresh_timeout
      |> maybe_free_stale_jobs
      |> restart_timer

    {:noreply, state}
  end
  def handle_info({:DOWN, _ref, :process, pid, reason}, state) do
    state = handle_worker_done(state, pid, reason)
    {:noreply, state}
  end

  defp maybe_handle_poll_timeout(state) do
    time_since_last_poll = Timex.diff(Timex.now, state.last_time_of_poll, :seconds)
    if time_since_last_poll >= state.poll_interval do
      state = handle_poll_timeout(state)
      %{state | last_time_of_poll: Timex.now}
    else
      state
    end
  end

  defp maybe_handle_job_refresh_timeout(state) do
    time_since_last_job_refresh = Timex.diff(Timex.now, state.last_time_of_job_refresh, :seconds)
    if time_since_last_job_refresh >= refresh_job_period() do
      state = handle_job_refresh(state)
      %{state | last_time_of_job_refresh: Timex.now}
    else
      state
    end
  end

  defp maybe_free_stale_jobs(state) do
    time_since_stale_reset = Timex.diff(Timex.now, state.last_time_of_stale_reset)
    if time_since_stale_reset >= ArbejdQ.stale_job_period do
      state = free_stale_jobs(state)
      %{state | last_time_of_stale_reset: Timex.now}
    else
      state
    end
  end

  @spec free_stale_jobs(state) :: state
  defp free_stale_jobs(state) do
    IO.puts "Free stale jobs"

    state.queues
    |> Enum.each(fn {queue, _} ->
      stale_jobs = ArbejdQ.list_stale_jobs(to_string(queue))
      Enum.each(stale_jobs, &release_job(&1))
    end)

    state
  end

  @spec release_job(Job.t) :: :ok
  defp release_job(job) do
    try do
      IO.puts "Releasing #{inspect job}"
      job
      |> Job.changeset(
        %{
          status: :queued,
          status_updated: DateTime.utc_now
        })
      |> ArbejdQ.repo().update!

      :ok
    rescue
      Ecto.StaleEntryError -> :ok
    end
  end

  @spec handle_poll_timeout(state) :: state
  @doc false
  def handle_poll_timeout(state) do
    schedule_jobs(state)
  end

  @spec handle_job_refresh(state) :: state
  @doc false
  def handle_job_refresh(state) do
    state.workers
    |> Enum.each(fn w ->
      case ArbejdQ.get_job(w.job_id) do
        {:ok, job} ->
          if job.status == :running do
            try do
              job
              |> Job.changeset(%{status_updated: DateTime.utc_now()})
              |> ArbejdQ.repo().update
              :ok
            rescue
              Ecto.StaleEntryError -> :ok
            end
          end
      end
    end)

    state
  end

  # Run through all queues in order of priority and try to
  # launch workers
  @doc false
  @spec schedule_jobs(state) :: state
  def schedule_jobs(state) do
    state.queues
    |> Enum.reduce(state, &fill_queue(&2, &1))
  end

  @spec fill_queue(state, {atom, queue_config}) :: state
  defp fill_queue(state, {queue, config}) do
    total_workers = max_workers(state)
    used_workers = worker_count(state)
    available_workers = total_workers - used_workers
    used_slots = worker_count(state, queue)
    total_slots = max_queue_jobs(config)
    available_slots = min(total_slots - used_slots, available_workers)

    IO.puts "Used slots     : #{to_string used_slots}"
    IO.puts "Total slots    : #{to_string total_slots}"
    IO.puts "Aavailable wrks: #{to_string available_workers}"
    IO.puts "Available slots: #{to_string available_slots}"

    jobs = ArbejdQ.list_queued_jobs(to_string(queue))

    {state, _} =
      jobs
      |> Enum.reduce({state, available_slots}, &try_execute_job(&2, &1, queue))

    state
  end

  @spec try_execute_job({state, non_neg_integer}, Job.t, atom) :: {state, non_neg_integer}
  defp try_execute_job({state, 0}, _, _), do: {state, 0}
  defp try_execute_job({state, remaining_slots}, job, queue) do
    try do
      {:ok, job} =
        Job.changeset(job, %{status: :running, status_updated: DateTime.utc_now})
        |> ArbejdQ.repo().update()

      scheduler_pid = self()

      {worker_pid, _} = spawn_monitor fn ->
        IO.puts "Running job #{inspect job}"
        result = job.worker_module.run(job.id, job.parameters)
        IO.puts "Ran job #{inspect job}"
        {:ok, job} = ArbejdQ.get_job(job.id)
        commit_result(job, result)
        GenServer.cast(scheduler_pid, {:job_done, job.id})
      end

      worker = %{
        pid: worker_pid,
        queue: queue,
        job_id: job.id,
      }

      state = %{state|
        workers: [worker | state.workers]
      }

      {state, remaining_slots - 1}
    rescue
      Ecto.StaleEntryError ->
        IO.puts "Job taken by someone else"
        {state, remaining_slots}
    end
  end

  # Calculates the wait time.
  # Returns it in milliseconds
  @spec wait_time(state) :: non_neg_integer
  defp wait_time(state) do
    now = Timex.now
    time_since_poll = Timex.diff(now, state.last_time_of_poll, :seconds)
    time_to_next_poll = max(0, state.poll_interval - time_since_poll)

    time_since_last_job_refresh = Timex.diff(now, state.last_time_of_job_refresh, :seconds)
    time_to_next_job_refresh = max(0, refresh_job_period() - time_since_last_job_refresh)

    wt = min(time_to_next_poll, time_to_next_job_refresh) * 1_000
    wt
  end

  defp restart_timer(state) do
    :timer.send_after(wait_time(state), :handle_timer)

    state
  end

  defp refresh_job_period, do: round(ArbejdQ.stale_job_period/2)

  @spec queue_workers(state, atom) :: [worker]
  defp queue_workers(state, queue) do
    state.workers
    |> Enum.filter(fn w -> w.queue == queue end)
  end

  @spec worker_count(state) :: non_neg_integer
  defp worker_count(state) do
    Enum.count(state.workers)
  end

  @spec worker_count(state, atom) :: non_neg_integer
  defp worker_count(state, queue) do
    queue_workers(state, queue)
    |> Enum.count
  end

  @spec max_queue_jobs(queue_config) :: non_neg_integer
  defp max_queue_jobs(config) do
    Keyword.get(config, :max_jobs, 1)
  end

  @spec max_workers(state) :: non_neg_integer
  defp max_workers(state) do
    Map.get(state, :max_jobs, 1)
  end

  @spec commit_result(Job.t, any) :: Job.t
  defp commit_result(%Job{status: :running} = job, result) do
    try do
      {:ok, job} =
        Job.changeset(job,
                      %{
                        status: :done,
                        status_updated: DateTime.utc_now,
                        result: result,
                      }
        )
        |> ArbejdQ.repo().update
      job
    rescue
      Ecto.StaleEntryError -> commit_result(ArbejdQ.repo().get(Job, job.id), result)
    end
  end
  defp commit_result(job, _result) do
    IO.puts "Result has already been posted for job #{job.id}"
    job
  end

  @spec commit_failure(Job.t, any) :: Job.t
  defp commit_failure(job, result) do
    try do
      {:ok, job} =
        Job.changeset(job,
                      %{
                        status: :failed,
                        status_updated: DateTime.utc_now,
                        result: result,
                      }
        )
        |> ArbejdQ.repo().update
      job
    rescue
      Ecto.StaleEntryError -> commit_result(job, result)
    end
  end

  @spec handle_worker_done(state, pid, any) :: state
  defp handle_worker_done(state, pid, reason) do
    {worker, state} = find_and_remove_worker(state, pid)

    case worker do
      nil ->
        state

      worker ->
        case reason do
          :normal ->
            # Result of job has been recorded
            state

          reason ->
            # Failure means that the result may not have been reported
            {:ok, job} = ArbejdQ.get_job(worker.job_id)
            commit_failure(job, reason)

            state
        end
    end
  end

  @spec find_and_remove_worker(state, pid) :: {worker, state}
  defp find_and_remove_worker(state, pid) do
    worker = Enum.find(state.workers, fn w -> w.pid == pid end)
    workers = List.delete(state.workers, worker)

    {worker, %{state | workers: workers}}
  end
end
