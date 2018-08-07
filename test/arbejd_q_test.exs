defmodule ArbejdQTest do
  use ArbejdQ.DataCase, async: false
  doctest ArbejdQ

  test "Enqueue job", _tags do
    assert {:ok, job} = ArbejdQ.enqueue_job("default", ArbejdQ.Test.Worker,
                                            %{
                                              duration: 1
                                            })
    [queued_job] = ArbejdQ.list_queued_jobs("default")
    assert job.id == queued_job.id

    parent = self()
    spawn fn ->
      Ecto.Adapters.SQL.Sandbox.allow(ArbejdQ.Test.Repo, parent, self())
      ArbejdQ.execute_job(queued_job)
    end

    ArbejdQ.wait(job.id)
  end

  test "Concurrent execution of job" do
    assert {:ok, job} = ArbejdQ.enqueue_job("default", ArbejdQ.Test.Worker,
                                            %{
                                              duration: 1
                                            })

    parent = self()
    spawn fn ->
      Ecto.Adapters.SQL.Sandbox.allow(ArbejdQ.Test.Repo, parent, self())
      res = ArbejdQ.execute_job(job)
      send parent, {:job_result, res}
    end

    spawn fn ->
      Ecto.Adapters.SQL.Sandbox.allow(ArbejdQ.Test.Repo, parent, self())
      res = ArbejdQ.execute_job(job)
      send parent, {:job_result, res}
    end

    assert {:ok, {:ok, 1}} == ArbejdQ.wait(job.id)

    res_1 =
      receive do
        {:job_result, res} -> res
      end

    res_2 =
      receive do
        {:job_result, res} -> res
      end

    assert res_1 == {:error, :invalid_status} or res_2 == {:error, :invalid_status}
  end

  test "Stale job detection", _tags do
    assert {:ok, job} = ArbejdQ.enqueue_job("default", ArbejdQ.Test.Worker,
                                            %{
                                              duration: 20
                                            })
    parent = self()
    spawn fn ->
      Ecto.Adapters.SQL.Sandbox.allow(ArbejdQ.Test.Repo, parent, self())
      ArbejdQ.execute_job(job)
    end

    Process.sleep(1_000)
    assert [] = ArbejdQ.list_stale_jobs("default")

    Process.sleep(11_000)

    assert [stale_job] = ArbejdQ.list_stale_jobs("default")
    IO.inspect stale_job
  end

end
