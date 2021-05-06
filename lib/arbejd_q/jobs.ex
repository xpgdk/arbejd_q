defmodule ArbejdQ.Jobs do

  alias ArbejdQ.Job

  @spec notify_if_failed(Job.t()) :: Job.t()
  def notify_if_failed(%Job{status: :failed} = job) do
    callback = Application.get_env(:arbejd_q, :job_failed_callback, nil)
    if callback != nil do
      spawn fn ->
        callback.(job)
      end
    end
    job
  end

  def notify_if_failed(job), do: job
end
