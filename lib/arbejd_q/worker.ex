defmodule ArbejdQ.Worker do
  @moduledoc """
  """

  alias ArbejdQ.Job

  defmacro __using__(_opts) do
    quote do
      @behaviour ArbejdQ.Worker
      import ArbejdQ.Worker
    end
  end

  @callback validate_params(map) :: {:ok, map} | :error
  @callback run(String.t, map) :: any

  @spec report_progress(String.t, any) :: Job.t
  def report_progress(job_id, progress) do
    {:ok, job} = ArbejdQ.get_job(job_id)
    try do
      {:ok, job} =
        job
        |> Job.changeset(
          %{
            progress: progress,
            status_updated: DateTime.utc_now(),
          })
        |> ArbejdQ.repo().update
      job
    rescue
      Ecto.StaleEntryError -> job
    end
  end
end
