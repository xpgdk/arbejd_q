defmodule ArbejdQ.Scheduler do
  @moduledoc """
  Responsible for scheduling jobs for execution.

  The scheduler is configured with a separate scheduling configuration for
  each queue, which is setup as an argument to `init/1`.
  """

  use GenServer

  alias ArbejdQ.{
    Execution,
    Job,
    Resources
  }

  @type queue_config :: [
          max_jobs: non_neg_integer,
          priority: non_neg_integer
        ]

  @type opt ::
          {:queues,
           %{
             required(atom) => queue_config
           }}
          | {:max_jobs, non_neg_integer()}
          | {:poll_interval, non_neg_integer()}
  @type opts :: [opt]

  @typep worker :: %{
           pid: pid,
           queue: atom,
           job_id: String.t()
         }

  @typep state :: %{
           queues: [{:atom, queue_config}],
           max_jobs: non_neg_integer,
           poll_interval: non_neg_integer,
           workers: [worker],
           last_time_of_poll: DateTime.t(),
           last_time_of_job_refresh: DateTime.t(),
           last_time_of_stale_reset: DateTime.t(),
           timer_ref: reference | nil,
           disable_timer: boolean,
           disable_execution: boolean
         }

  @type global_scheduler_info :: %{
          total_workers: non_neg_integer,
          used_workers: non_neg_integer
        }

  @type per_queue_scheduler_info :: %{
          total_slots: non_neg_integer,
          used_slots: non_neg_integer
        }

  @type scheduler_info :: %{
          global: global_scheduler_info,
          queues: %{
            required(atom) => per_queue_scheduler_info
          }
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
  @spec start_link(opts, GenServer.options()) :: GenServer.on_start()
  def start_link(opts, gen_server_opts \\ []) do
    GenServer.start_link(__MODULE__, opts, gen_server_opts)
  end

  def child_spec([opts, genserver_opts]) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts, genserver_opts]}
    }
  end

  @doc """
  Start the scheduler.

  See `start_link/2`.
  """
  @spec start(opts, GenServer.options()) :: GenServer.on_start()
  def start(opts, gen_server_opts \\ []) do
    GenServer.start(__MODULE__, opts, gen_server_opts)
  end

  @spec stop(GenServer.server()) :: :ok
  def stop(pid) do
    GenServer.stop(pid)
  end

  @spec get_scheduler_info(GenServer.server()) :: scheduler_info
  def get_scheduler_info(pid) do
    GenServer.call(pid, :get_scheduler_info)
  end

  @spec poll_for_jobs(GenServer.server()) :: :ok
  def poll_for_jobs(pid) do
    GenServer.cast(pid, :poll_for_jobs)
  end

  @spec disable_timer(GenServer.server()) :: :ok
  def disable_timer(pid) do
    GenServer.call(pid, :disable_timer)
  end

  @spec enable_timer(GenServer.server()) :: :ok
  def enable_timer(pid) do
    GenServer.call(pid, :enable_timer)
  end

  @spec enable_execution(GenServer.server()) :: :ok
  def enable_execution(pid) do
    GenServer.call(pid, :enable_execution)
  end

  @spec disable_execution(GenServer.server()) :: :ok
  def disable_execution(pid) do
    GenServer.call(pid, :disable_execution)
  end

  @spec reconfigure(GenServer.server(), opts) :: :ok
  def reconfigure(pid, opts) do
    GenServer.call(pid, {:reconfigure, opts})
  end

  ### GenServer Callback functions ###
  @spec init(opts) :: {:ok, state}
  def init(opts) do
    initial_state =
      do_reconfigure(
        %{
          queues: [],
          max_jobs: 0,
          poll_interval: 0,
          workers: [],
          last_time_of_poll: Timex.now(),
          last_time_of_job_refresh: Timex.now(),
          last_time_of_stale_reset: Timex.now(),
          timer_ref: nil,
          disable_timer: Application.get_env(:arbejd_q, :disable_timer, false),
          disable_execution: Application.get_env(:arbejd_q, :disable_execution, false)
        },
        opts
      )

    {:ok, restart_timer(initial_state)}
  end

  def handle_cast({:job_done, _job_id}, state) do
    {:noreply, schedule_jobs(state)}
  end

  def handle_cast(:poll_for_jobs, state) do
    state =
      state
      |> schedule_jobs

    {:noreply, state}
  end

  def handle_call(:get_scheduler_info, _sender, state) do
    {:reply, calculate_scheduler_info(state), state}
  end

  def handle_call({:reconfigure, opts}, _sender, state) do
    {:reply, :ok, do_reconfigure(state, opts)}
  end

  def handle_call(:disable_timer, _sender, state) do
    if state.timer_ref != nil do
      Process.cancel_timer(state.timer_ref)
    end

    {:reply, :ok, %{state | timer_ref: nil, disable_timer: true}}
  end

  def handle_call(:enable_timer, _sender, state) do
    new_state =
      %{state | disable_timer: false}
      |> restart_timer

    {:reply, :ok, new_state}
  end

  def handle_call(:enable_execution, _sender, state) do
    new_state =
      %{state | disable_execution: false}
      |> schedule_jobs

    {:reply, :ok, new_state}
  end

  def handle_call(:disable_execution, _sender, state) do
    new_state = %{state | disable_execution: true}
    {:reply, :ok, new_state}
  end

  def handle_info(:handle_timer, state) do
    state =
      %{state | timer_ref: nil}
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

  ### Internal functions ###

  defp maybe_handle_poll_timeout(state) do
    time_since_last_poll = Timex.diff(Timex.now(), state.last_time_of_poll, :seconds)

    if time_since_last_poll >= state.poll_interval do
      state = handle_poll_timeout(state)
      %{state | last_time_of_poll: Timex.now()}
    else
      state
    end
  end

  defp maybe_handle_job_refresh_timeout(state) do
    time_since_last_job_refresh =
      Timex.diff(Timex.now(), state.last_time_of_job_refresh, :seconds)

    if time_since_last_job_refresh >= refresh_job_period() do
      state = handle_job_refresh(state)
      %{state | last_time_of_job_refresh: Timex.now()}
    else
      state
    end
  end

  defp maybe_free_stale_jobs(state) do
    time_since_stale_reset = Timex.diff(Timex.now(), state.last_time_of_stale_reset)

    if time_since_stale_reset >= ArbejdQ.stale_job_period() do
      state = free_stale_jobs(state)
      %{state | last_time_of_stale_reset: Timex.now()}
    else
      state
    end
  end

  @spec free_stale_jobs(state) :: state
  defp free_stale_jobs(state) do
    state.queues
    |> Enum.each(fn {queue, _} ->
      stale_jobs = ArbejdQ.list_stale_jobs(to_string(queue))
      Enum.each(stale_jobs, &release_job(&1))
    end)

    state
  end

  @spec release_job(Job.t()) :: :ok
  defp release_job(%Job{stale_counter: stale_counter} = job) do
    try do
      new_status =
        if stale_counter > 1 do
          :failed
        else
          :queued
        end

      ArbejdQ.repo().transaction(fn ->
        job
        |> Job.changeset(%{
          status: new_status,
          status_updated: DateTime.utc_now(),
          stale_counter: stale_counter + 1
        })
        |> ArbejdQ.repo().update!
        |> Resources.free_resources()
      end)
    rescue
      Ecto.StaleEntryError ->
        :ok
    end
  end

  @spec handle_poll_timeout(state) :: state
  @doc false
  def handle_poll_timeout(state) do
    state
    |> schedule_jobs
    |> cleanup_jobs
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
  def schedule_jobs(%{disable_execution: false} = state) do
    state.queues
    |> Enum.reduce(state, &fill_queue(&2, &1))
  end

  def schedule_jobs(state), do: state

  @doc false
  @spec cleanup_jobs(state) :: state
  def cleanup_jobs(state) do
    state.queues
    |> Enum.reduce(state, &cleanup_queue(&2, &1))
  end

  @spec cleanup_queue(state, {atom, queue_config}) :: state
  defp cleanup_queue(state, {queue, _config}) do
    {_count, _removed} =
      ArbejdQ.Job.list_expired_jobs(to_string(queue), Timex.now())
      |> Ecto.Query.exclude(:order_by)
      |> ArbejdQ.repo().delete_all()

    state
  end

  @spec calculate_worker_numbers(state) :: global_scheduler_info
  defp calculate_worker_numbers(state) do
    %{
      total_workers: max_workers(state),
      used_workers: worker_count(state)
    }
  end

  @spec calculate_queue_numbers(state, {atom, Keyword.t()}) :: per_queue_scheduler_info
  defp calculate_queue_numbers(state, {queue, config}) do
    %{
      used_slots: worker_count(state, queue),
      total_slots: max_queue_jobs(config)
    }
  end

  @doc false
  @spec calculate_scheduler_info(state) :: scheduler_info
  def calculate_scheduler_info(state) do
    queues =
      state.queues
      |> Enum.map(fn
        {queue, config} -> {queue, calculate_queue_numbers(state, {queue, config})}
      end)
      |> Map.new()

    %{
      global: calculate_worker_numbers(state),
      queues: queues
    }
  end

  @spec fill_queue(state, {atom, queue_config}) :: state
  defp fill_queue(state, {queue, config}) do
    workers = calculate_worker_numbers(state)

    total_workers = workers.total_workers
    used_workers = workers.used_workers
    available_workers = total_workers - used_workers

    queue_slots = calculate_queue_numbers(state, {queue, config})

    used_slots = queue_slots.used_slots
    total_slots = queue_slots.total_slots
    available_slots = min(total_slots - used_slots, available_workers)

    if available_slots > 0 do
      jobs = ArbejdQ.list_queued_jobs(to_string(queue))

      {state, _} =
        jobs
        |> Enum.reduce({state, available_slots}, &try_execute_job(&2, &1, queue))

      state
    else
      state
    end
  end

  @spec try_execute_job({state, non_neg_integer}, Job.t(), atom) :: {state, non_neg_integer}
  defp try_execute_job({state, 0}, _, _), do: {state, 0}

  defp try_execute_job({state, remaining_slots}, job, queue) do
    with {:ok, job} <- Execution.take_job(job) do
      scheduler_pid = self()

      {worker_pid, _} =
        spawn_monitor(fn ->
          case Execution.execute_job(job) do
            {:ok, job, _result} ->
              GenServer.cast(scheduler_pid, {:job_done, job.id})

            {:error, _reason} ->
              :ok
          end
        end)

      worker = %{
        pid: worker_pid,
        queue: queue,
        job_id: job.id
      }

      state = %{state | workers: [worker | state.workers]}

      {state, remaining_slots - 1}
    else
      {:error, reason} when reason in [:taken, :missing_resources] ->
        {state, remaining_slots}
    end
  end

  # Calculates the wait time.
  # The resulting wait time is the minimum time until:
  #  a) Check for new jobs (poll_interval).
  #  b) Refresh running jobs status (ArbejdQ.stale_job_period/2).
  #
  # Returns wait time in milliseconds
  @spec wait_time(state) :: non_neg_integer
  defp wait_time(state) do
    now = Timex.now()
    time_since_poll = Timex.diff(now, state.last_time_of_poll, :seconds)
    time_to_next_poll = max(0, state.poll_interval - time_since_poll)

    time_since_last_job_refresh = Timex.diff(now, state.last_time_of_job_refresh, :seconds)
    time_to_next_job_refresh = max(0, refresh_job_period() - time_since_last_job_refresh)

    wt = min(time_to_next_poll, time_to_next_job_refresh) * 1_000
    wt
  end

  @spec restart_timer(state) :: state
  defp restart_timer(%{timer_ref: nil, disable_timer: false} = state) do
    timer_ref = Process.send_after(self(), :handle_timer, wait_time(state))

    %{state | timer_ref: timer_ref}
  end

  defp restart_timer(state), do: state

  defp refresh_job_period, do: round(ArbejdQ.stale_job_period() / 2)

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
    |> Enum.count()
  end

  @spec max_queue_jobs(Keyword.t()) :: non_neg_integer
  defp max_queue_jobs(config) do
    Keyword.get(config, :max_jobs, 1)
  end

  @spec max_workers(state) :: non_neg_integer
  defp max_workers(state) do
    Map.get(state, :max_jobs, 1)
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
            Execution.commit_failure(job, reason)

            state
        end
    end
  end

  @spec find_and_remove_worker(state, pid) :: {worker | nil, state}
  defp find_and_remove_worker(state, pid) do
    worker = Enum.find(state.workers, fn w -> w.pid == pid end)
    workers = List.delete(state.workers, worker)

    {worker, %{state | workers: workers}}
  end

  @spec do_reconfigure(state, opts) :: state
  defp do_reconfigure(state, opts) do
    queues =
      Keyword.get(opts, :queues, [])
      |> Enum.sort_by(
        fn {_k, v} ->
          Keyword.get(v, :priority, 1)
        end,
        &>/2
      )

    max_jobs = Keyword.get(opts, :max_jobs, 0)
    poll_interval = Keyword.get(opts, :poll_interval, 30)

    Map.merge(
      state,
      %{
        max_jobs: max_jobs,
        poll_interval: poll_interval,
        queues: queues
      }
    )
  end
end
