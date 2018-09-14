defmodule ArbejdQ.ExeutionTest do
  use ArbejdQ.DataCase

  alias ArbejdQ.Job

  test "Refresh of job, when assigning job" do
    assert {:ok, job} = ArbejdQ.enqueue_job("default", ArbejdQ.Test.Worker,
                                            %{
                                              duration: 1
                                            })
    {:ok, job} = ArbejdQ.Execution.take_job(job)

    job
    |> Job.changeset(%{status_updated: DateTime.utc_now()})
    |> ArbejdQ.repo().update

    assert {:ok, _job, _} = ArbejdQ.Execution.execute_job(job)
  end
end
