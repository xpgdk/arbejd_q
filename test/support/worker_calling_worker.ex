defmodule ArbejdQ.Test.WorkerCallingWorker do
  @moduledoc false

  use ArbejdQ.Worker

  @spec validate_params(map) :: {:ok, map} | :error
  def validate_params(%{queue: queue, parameters: parameters} = params)
      when is_binary(queue) and is_map(parameters),
      do: {:ok, params}

  def validate_params(_), do: :error

  @spec run(String.t(), map) :: any
  def run(job_id, %{queue: queue, parameters: parameters} = _params) do
    report_progress(job_id, 1)

    {:ok, :done, {:ok, result}} =
      ArbejdQ.enqueue_and_wait(queue, ArbejdQ.Test.Worker, parameters, [], :infinity)

    {:ok, result}
  end
end
