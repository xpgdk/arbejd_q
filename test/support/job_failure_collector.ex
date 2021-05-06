defmodule ArbejdQ.JobFailureCollector do
  def start_link() do
    Agent.start_link(fn -> [] end, name: __MODULE__)
  end

  def stop() do
    Agent.stop(__MODULE__)
  end

  def get_failed_jobs() do
    Agent.get(__MODULE__, fn jobs -> jobs end)
  end

  def clear() do
    Agent.update(__MODULE__, fn _ -> [] end)
  end

  def job_failed(job) do
    pid = Process.whereis(__MODULE__)
    if pid != nil do
      Agent.update(__MODULE__, fn failed_jobs -> [job | failed_jobs] end)
    end
    :ok
  end
end
