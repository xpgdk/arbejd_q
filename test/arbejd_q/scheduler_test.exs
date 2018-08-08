defmodule ArbejdQ.SchedulerTest do
  use ArbejdQ.DataCase, async: false

  alias ArbejdQ.{
    Scheduler,
    Job,
  }

  setup do
    Ecto.Adapters.SQL.Sandbox.mode(ArbejdQ.Test.Repo, {:shared, self()})
  end

  defp named_scheduler(tags) do
    {:ok, pid} = Scheduler.start_link(
      [
        poll_interval: 60 * 60, # So high that it will never occur during the test
        max_jobs: 1,
        queues: [
          normal: [
            max_jobs: 1,
            priority: 1,
          ],
          high: [
            max_jobs: 1,
            priority: 10,
          ],
        ],
      ], name: :arbejd_q_scheduler)


    on_exit fn ->
      # Ensure that the Scheduler does not run after the test has been completed
      Process.exit(pid, :kill)
    end

    Map.merge(
      tags,
      %{
        pid: pid
      })
  end

  defp scheduler(tags) do
    {:ok, pid} = Scheduler.start_link(
      [
        poll_interval: 1,
        max_jobs: 1,
        queues: [
          normal: [
            max_jobs: 1,
            priority: 1,
          ],
          high: [
            max_jobs: 1,
            priority: 10,
          ],
        ],
      ])


    on_exit fn ->
      # Ensure that the Scheduler does not run after the test has been completed
      Process.exit(pid, :kill)
    end

    Map.merge(
      tags,
      %{
        pid: pid
      })
  end

  defp dummy_state(tags) do
    state = %{
      workers: [],
      poll_interval: 10_000,
      max_jobs: 3,
      queues: [
        high: [
          max_jobs: 1,
          priority: 10,
        ],
        normal: [
          max_jobs: 2,
          priority: 1,
        ]
      ]
    }

    Map.merge(
      tags,
      %{
        state: state
      })
  end

  describe "Running named scheduler" do
    setup [:named_scheduler]

    test "Basic scheduler test", _tags do
      {:ok, job} =
        ArbejdQ.enqueue_job("normal",
                            ArbejdQ.Test.Worker,
                            %{
                              duration: 1
                            })
      {:ok, :done, _result} = ArbejdQ.wait(job.id)
    end
  end

  describe "Running scheduler" do
    setup [:scheduler]

    test "Basic scheduler test", _tags do
      {:ok, job} =
        ArbejdQ.enqueue_job("normal",
                            ArbejdQ.Test.Worker,
                            %{
                              duration: 1
                            })
      {:ok, :done, _result} = ArbejdQ.wait(job.id)

      Process.sleep(6000)

      # Expect job to be removed as it has a expiration_duration of 5 seconds
      assert {:error, :not_found} = ArbejdQ.get_job(job)
    end

    test "Stale jobs", _tags do
      job =
        Job.build("normal",
                  ArbejdQ.Test.Worker,
                  %{duration: 1},
                  %{
                    status: :running,
                    status_updated: DateTime.utc_now
                  }
        )
        |> ArbejdQ.repo().insert!

      # After 5 seconds, the Scheduler ought to free the job
      # and shortly thereafter execute it
      {:ok, :done, result} = ArbejdQ.wait(job.id)
      assert {:ok, 1} == result

      Process.sleep(6000)

      # Expect job to be removed as it has a expiration_duration of 5 seconds
      assert {:error, :not_found} = ArbejdQ.get_job(job)
    end

    test "Concurrent results", _tags do
      {:ok, job} =
        ArbejdQ.enqueue_job("normal",
                            ArbejdQ.Test.Worker,
                            %{
                              duration: 2
                            })
      # Wait long enough for the scheduler to start executing
      # the job
      Process.sleep(1200)

      {:ok, job} = ArbejdQ.get_job(job.id)

      assert job.status == :running

      # Now inject a result before the job completes.
      job
      |> Job.changeset(
        %{
          status: :done,
          result: 42
        }
      )
      |> ArbejdQ.repo().update!

      # Wait long enough for the job completion
      Process.sleep(1000)

      # The result is the injected one, as it was the first result.
      {:ok, :done, result} = ArbejdQ.wait(job.id)
      assert result == 42
    end

    test "Error in worker", _tags do
      {:ok, job} =
        ArbejdQ.enqueue_job("normal",
                            ArbejdQ.Test.Worker,
                            %{
                              duration: "abekat"
                            })
      assert {:ok, :failed, reason} = ArbejdQ.wait(job)
      assert {:badarith, _} = reason
    end
  end

  describe "Dummy state" do
    setup [:dummy_state]

    test "schedule_jobs/1", tags do
      assert {:ok, job_1} =
        ArbejdQ.enqueue_job("normal",
                            ArbejdQ.Test.Worker,
                            %{
                              duration: 1,
                            })

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
          used_workers: 0
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
end
