defmodule ArbejdQ.Test.Worker do
  @moduledoc false

  use ArbejdQ.Worker

  @spec validate_params(map) :: {:ok, map} | :error
  def validate_params(%{duration: _} = params), do: {:ok, params}
  def validate_params(_), do: :error

  @spec run(String.t, map) :: any
  def run(job_id, %{duration: duration}) do
    report_progress(job_id, 1)
    Process.sleep(duration * 1_000)

    {:ok, duration}
  end
end
