defmodule ArbejdQTest do
  use ArbejdQ.DataCase, async: false
  doctest ArbejdQ

  alias ArbejdQ.Execution

  test "Enqueue job", _tags do
    assert {:ok, job} =
             ArbejdQ.enqueue_job("default", ArbejdQ.Test.Worker, %{
               duration: 1
             })

    [queued_job] = ArbejdQ.list_queued_jobs("default")
    assert job.id == queued_job.id

    parent = self()

    spawn_link(fn ->
      Ecto.Adapters.SQL.Sandbox.allow(ArbejdQ.Test.Repo, parent, self())
      {:ok, job} = Execution.take_job(queued_job)
      Execution.execute_job(job)
    end)

    assert {:ok, :done, {:ok, 1}} == ArbejdQ.wait(job.id)

    assert {:ok, job} = ArbejdQ.get_job(job)
    assert job.expiration_time != nil
    assert_in_delta Timex.diff(job.expiration_time, job.completion_time, :seconds), 5, 1
  end

  test "Concurrent execution of job" do
    assert {:ok, job} =
             ArbejdQ.enqueue_job("default", ArbejdQ.Test.Worker, %{
               duration: 1
             })

    parent = self()

    spawn_link(fn ->
      Ecto.Adapters.SQL.Sandbox.allow(ArbejdQ.Test.Repo, parent, self())

      result =
        case Execution.take_job(job) do
          {:ok, job} ->
            {:ok, _, res} = Execution.execute_job(job)
            res

          other ->
            other
        end

      send(parent, {:job_result, result})
    end)

    spawn_link(fn ->
      Ecto.Adapters.SQL.Sandbox.allow(ArbejdQ.Test.Repo, parent, self())

      result =
        case Execution.take_job(job) do
          {:ok, job} ->
            {:ok, _, res} = Execution.execute_job(job)
            res

          other ->
            other
        end

      send(parent, {:job_result, result})
    end)

    assert {:ok, :done, {:ok, 1}} == ArbejdQ.wait(job.id)

    res_1 =
      receive do
        {:job_result, res} -> res
      end

    res_2 =
      receive do
        {:job_result, res} -> res
      end

    assert res_1 == {:error, :taken} or res_2 == {:error, :taken}
    assert res_1 == {:ok, 1} or res_2 == {:ok, 1}
  end

  test "Stale job detection", _tags do
    assert {:ok, job} =
             ArbejdQ.enqueue_job("default", ArbejdQ.Test.Worker, %{
               duration: 20
             })

    parent = self()

    spawn_link(fn ->
      Ecto.Adapters.SQL.Sandbox.allow(ArbejdQ.Test.Repo, parent, self())
      {:ok, job} = Execution.take_job(job)
      {:ok, _job, _result} = Execution.execute_job(job)
    end)

    Process.sleep(1_000)
    assert [] = ArbejdQ.list_stale_jobs("default")

    Process.sleep(11_000)

    assert [stale_job] = ArbejdQ.list_stale_jobs("default")
    assert stale_job.id == job.id
  end

  describe "Scheduler with one queue and one slot" do
    setup do
      Ecto.Adapters.SQL.Sandbox.mode(ArbejdQ.Test.Repo, {:shared, self()})

      {:ok, pid} =
        ArbejdQ.Scheduler.start_link(
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

      %{
        pid: pid
      }
    end

    test "Calling enqueue_and_wait from a running worker will run the nested job synchronously", _tags do
      # Only one job can be run at a time, so if the worker enqueues any jobs, the test will
      # time out.
      assert {:ok, job} =
               ArbejdQ.enqueue_job("normal", ArbejdQ.Test.WorkerCallingWorker, %{
                 queue: "normal",
                 parameters: %{duration: 1}
               }, [])

      assert {:ok, :done, {:ok, 1}} = ArbejdQ.wait(job)
    end

    test "Calling enqueue_and_wait from a running worker will run the nested job synchronously (1)", _tags do
      # Only one job can be run at a time, so if the worker enqueues any jobs, the test will
      # time out.
      assert {:ok, :done, {:ok, 1}} =
               ArbejdQ.enqueue_and_wait("normal", ArbejdQ.Test.WorkerCallingWorker, %{
                 queue: "normal",
                 parameters: %{duration: 1}
               }, [])
    end
  end
end
