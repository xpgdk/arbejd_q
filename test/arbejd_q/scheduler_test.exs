defmodule ArbejdQ.SchedulerTest do
  use ArbejdQ.DataCase, async: false

  alias ArbejdQ.{
    Scheduler,
    Job
  }

  setup do
    Ecto.Adapters.SQL.Sandbox.mode(ArbejdQ.Test.Repo, {:shared, self()})
  end

  defp named_scheduler(tags) do
    {:ok, pid} =
      Scheduler.start_link(
        [
          # So high that it will never occur during the test
          poll_interval: 60 * 60,
          max_jobs: 1,
          queues: [
            normal: [
              max_jobs: 1,
              priority: 1
            ],
            high: [
              max_jobs: 1,
              priority: 10
            ]
          ]
        ],
        name: :arbejd_q_scheduler
      )

    on_exit(fn ->
      # Ensure that the Scheduler does not run after the test has been completed
      Process.exit(pid, :kill)
    end)

    Map.merge(
      tags,
      %{
        pid: pid
      }
    )
  end

  defp scheduler(tags, opts \\ []) do
    {:ok, pid} =
      Scheduler.start_link(
        Keyword.merge(
          [
            failed_job_handler: ArbejdQ.Test.FailureHandler,
            poll_interval: 1,
            max_jobs: 1,
            queues: [
              normal: [
                max_jobs: 1,
                priority: 1
              ],
              high: [
                max_jobs: 1,
                priority: 10
              ]
            ]
          ],
          opts
        )
      )

    {:ok, failed_job_agent_pid} = ArbejdQ.Test.FailureHandler.start_link()

    on_exit(fn ->
      # Ensure that the Scheduler does not run after the test has been completed
      Process.exit(pid, :kill)
      Process.exit(failed_job_agent_pid, :kill)
    end)

    Map.merge(
      tags,
      %{
        pid: pid
      }
    )
  end

  defp dummy_state(tags) do
    state = %{
      workers: [],
      poll_interval: 10_000,
      max_jobs: 3,
      queues: [
        high: [
          max_jobs: 1,
          priority: 10
        ],
        normal: [
          max_jobs: 2,
          priority: 1
        ]
      ],
      disable_timer: false,
      disable_execution: false
    }

    Map.merge(
      tags,
      %{
        state: state
      }
    )
  end

  describe "Running named scheduler" do
    setup [:named_scheduler]

    test "Basic scheduler test", _tags do
      {:ok, job} =
        ArbejdQ.enqueue_job(
          "normal",
          ArbejdQ.Test.Worker,
          %{
            duration: 1
          }
        )

      {:ok, :done, _result} = ArbejdQ.wait(job.id)
    end

    test "Nonexisting worker", _tags do
      assert_raise UndefinedFunctionError, fn ->
        ArbejdQ.enqueue_job("normal", NonExisting.Worker, %{})
      end
    end
  end

  describe "Running scheduler" do
    setup [:scheduler]

    test "Basic scheduler test", _tags do
      {:ok, job} =
        ArbejdQ.enqueue_job(
          "normal",
          ArbejdQ.Test.Worker,
          %{
            duration: 1
          }
        )

      {:ok, :done, _result} = ArbejdQ.wait(job.id)

      Process.sleep(6000)

      # Expect job to be removed as it has a expiration_duration of 5 seconds
      assert {:error, :not_found} = ArbejdQ.get_job(job)
    end

    test "Nonexisting worker injected into database", _tags do
      assert {:ok, job} = ArbejdQ.create_job("normal", NonExisting.Worker, %{})

      assert {:ok, :failed, _reason} = ArbejdQ.wait(job)
    end

    test "Stale jobs", _tags do
      query =
        Job.build(
          "normal",
          ArbejdQ.Test.Worker,
          %{duration: 1},
          %{
            status: :running,
            status_updated: DateTime.utc_now()
          }
        )

      job =
        case ArbejdQ.repo().transaction(query) do
          {:ok, changes} -> changes[:update]
        end

      # After 5 seconds, the Scheduler ought to free the job
      # and shortly thereafter execute it
      {:ok, :done, result} = ArbejdQ.wait(job.id)
      assert {:ok, 1} == result

      Process.sleep(6000)

      # Expect job to be removed as it has a expiration_duration of 5 seconds
      assert {:error, :not_found} = ArbejdQ.get_job(job)
    end

    test "Stale job with stale_counter = 2 cause job to fail", _tags do
      query =
        Job.build(
          "normal",
          ArbejdQ.Test.Worker,
          %{duration: 1},
          %{
            status: :running,
            stale_counter: 2,
            status_updated: DateTime.utc_now()
          }
        )

      job =
        case ArbejdQ.repo().transaction(query) do
          {:ok, changes} -> changes[:update]
        end

      {:ok, :failed, nil} = ArbejdQ.wait(job.id)

      job_id = job.id
      assert [{:stale, %Job{id: ^job_id}}] = ArbejdQ.Test.FailureHandler.get_failed_jobs()
    end

    test "Concurrent results", _tags do
      {:ok, job} =
        ArbejdQ.enqueue_job(
          "normal",
          ArbejdQ.Test.Worker,
          %{
            duration: 2
          }
        )

      # Wait long enough for the scheduler to start executing
      # the job
      Process.sleep(1200)

      {:ok, job} = ArbejdQ.get_job(job.id)

      assert job.status == :running

      # Now inject a result before the job completes.
      job
      |> Job.changeset(%{
        status: :done,
        result: 42
      })
      |> ArbejdQ.repo().update!

      # Wait long enough for the job completion
      Process.sleep(1000)

      # The result is the injected one, as it was the first result.
      {:ok, :done, result} = ArbejdQ.wait(job.id)
      assert result == 42
    end

    test "Error in worker", _tags do
      ArbejdQ.JobFailureCollector.start_link()
      ArbejdQ.JobFailureCollector.clear()

      {:ok, job} =
        ArbejdQ.enqueue_job(
          "normal",
          ArbejdQ.Test.Worker,
          %{
            duration: "abekat"
          }
        )

      assert {:ok, :failed, reason} = ArbejdQ.wait(job)
      assert %ArithmeticError{message: "bad argument in arithmetic expression"} = reason

      job_id = job.id
      assert [{:run_failure, %Job{id: ^job_id}}] = ArbejdQ.Test.FailureHandler.get_failed_jobs()
    end
  end

  describe "Dummy state" do
    setup [:dummy_state]

    test "schedule_jobs/1", tags do
      assert {:ok, job_1} =
               ArbejdQ.enqueue_job(
                 "normal",
                 ArbejdQ.Test.Worker,
                 %{
                   duration: 1
                 }
               )

      new_state = Scheduler.schedule_jobs(tags.state)
      assert [worker] = new_state.workers
      assert worker.queue == :normal

      receive do
        {:"$gen_cast", {:job_done, id}} ->
          assert id == job_1.id
      end
    end

    test "get_scheduler_info", tags do
      info = Scheduler.calculate_scheduler_info(tags.state)

      assert info == %{
               global: %{
                 total_workers: 3,
                 used_workers: 0,
                 execution_enabled?: true,
                 timer_enabled?: true
               },
               queues: %{
                 high: %{
                   total_slots: 1,
                   used_slots: 0
                 },
                 normal: %{
                   total_slots: 2,
                   used_slots: 0
                 }
               }
             }
    end
  end

  describe "Timer and Scheduler Configuration" do
    test "Initial state has both timer and execution enabled by default" do
      tags = scheduler(%{})
      info = Scheduler.get_scheduler_info(tags.pid)

      assert %{
               global: %{
                 execution_enabled?: true,
                 timer_enabled?: true
               }
             } = info
    end

    test "Application environment controls execution" do
      Application.put_env(:arbejd_q, :disable_execution, true)

      tags = scheduler(%{})
      info = Scheduler.get_scheduler_info(tags.pid)

      Application.delete_env(:arbejd_q, :disable_execution)

      assert %{
               global: %{
                 execution_enabled?: false,
                 timer_enabled?: true
               }
             } = info
    end

    test "Application environment controls timer" do
      Application.put_env(:arbejd_q, :disable_timer, true)

      tags = scheduler(%{})
      info = Scheduler.get_scheduler_info(tags.pid)

      Application.delete_env(:arbejd_q, :disable_timer)

      assert %{
               global: %{
                 execution_enabled?: true,
                 timer_enabled?: false
               }
             } = info
    end

    test "Execution can be disabled as start option" do
      tags = scheduler(%{}, disable_execution: true)
      info = Scheduler.get_scheduler_info(tags.pid)

      assert %{
               global: %{
                 execution_enabled?: false,
                 timer_enabled?: true
               }
             } = info
    end

    test "Timer can be disabled as start option" do
      tags = scheduler(%{}, disable_timer: true)
      info = Scheduler.get_scheduler_info(tags.pid)

      assert %{
               global: %{
                 execution_enabled?: true,
                 timer_enabled?: false
               }
             } = info
    end

    test "execution: start option overrides application env -- 1" do
      Application.put_env(:arbejd_q, :disable_execution, false)

      tags = scheduler(%{}, disable_execution: true)
      info = Scheduler.get_scheduler_info(tags.pid)

      Application.delete_env(:arbejd_q, :disable_execution)

      assert %{
               global: %{
                 execution_enabled?: false,
                 timer_enabled?: true
               }
             } = info
    end

    test "execution: start option overrides application env -- 2" do
      Application.put_env(:arbejd_q, :disable_execution, true)

      tags = scheduler(%{}, disable_execution: false)
      info = Scheduler.get_scheduler_info(tags.pid)

      Application.delete_env(:arbejd_q, :disable_execution)

      assert %{
               global: %{
                 execution_enabled?: true,
                 timer_enabled?: true
               }
             } = info
    end

    test "timer: start option overrides application env -- 1" do
      Application.put_env(:arbejd_q, :disable_timer, false)

      tags = scheduler(%{}, disable_timer: true)
      info = Scheduler.get_scheduler_info(tags.pid)

      Application.delete_env(:arbejd_q, :disable_timer)

      assert %{
               global: %{
                 execution_enabled?: true,
                 timer_enabled?: false
               }
             } = info
    end

    test "timer: start option overrides application env -- 2" do
      Application.put_env(:arbejd_q, :disable_timer, true)

      tags = scheduler(%{}, disable_timer: false)
      info = Scheduler.get_scheduler_info(tags.pid)

      Application.delete_env(:arbejd_q, :disable_timer)

      assert %{
               global: %{
                 execution_enabled?: true,
                 timer_enabled?: true
               }
             } = info
    end
  end
end
