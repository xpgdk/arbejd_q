defmodule ArbejdQ.SchedulerTest do
  use ArbejdQ.DataCase, async: false

  alias ArbejdQ.{
    Scheduler,
    Job,
  }

  setup do
    Ecto.Adapters.SQL.Sandbox.mode(ArbejdQ.Test.Repo, {:shared, self()})
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

    IO.puts "Scheduler starter with #{inspect pid}"

    #    on_exit fn ->
    #      IO.puts "Stopping scheduler"
    #      Scheduler.stop(pid)
    #    end

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

  describe "Running scheduler" do
    setup [:scheduler]

    test "Basic scheduler test", _tags do
      {:ok, job} =
        ArbejdQ.enqueue_job("normal",
                            ArbejdQ.Test.Worker,
                            %{
                              duration: 1
                            })
      {:ok, result} = ArbejdQ.wait(job.id)
      IO.inspect result
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
      {:ok, result} = ArbejdQ.wait(job.id)
      IO.inspect result
    end

    test "Concurrent results", _tags do
      {:ok, job} =
        ArbejdQ.enqueue_job("normal",
                            ArbejdQ.Test.Worker,
                            %{
                              duration: 2
                            })
      Process.sleep(1000)

      {:ok, job} = ArbejdQ.get_job(job.id)

      job
      |> Job.changeset(
        %{
          status: :done,
          result: 42
        }
      )
      |> ArbejdQ.repo().update!

      Process.sleep(2000)

      {:ok, result} = ArbejdQ.wait(job.id)
      assert result == 42
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
      IO.inspect worker

      Scheduler.handle_job_refresh(new_state)

      receive do
        {:"$gen_cast", {:job_done, id}} ->
          IO.puts "Job done: #{id}"
          IO.inspect job_1
      end
    end
  end
end
