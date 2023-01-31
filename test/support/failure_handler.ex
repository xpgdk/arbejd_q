defmodule ArbejdQ.Test.FailureHandler do
  @behaviour ArbejdQ.FailedJobHandler

  alias ArbejdQ.Job

  def job_failed(%Job{} = job, trigger) do
    Agent.update(__MODULE__, fn failed_jobs ->
      [{trigger, job} | failed_jobs]
    end)

    :ok
  end

  def start_link() do
    Agent.start_link(fn -> [] end, name: __MODULE__)
  end

  def get_failed_jobs() do
    Agent.get(__MODULE__, fn failed_jobs -> failed_jobs end)
  end
end
