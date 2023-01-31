defmodule ArbejdQ.FailedJobHandler do
  @moduledoc """
  Behaviour module used for handling failed jobs.
  """

  @typedoc """
  Describes the triggering of the error handler.

  `:stale` means that the job was found to be stale (i.e. left behind by someone).
  `:run_failure` means that the job failed running.
  """
  @type trigger :: :stale | :run_failure

  @callback job_failed(job :: ArbejdQ.Job.t(), trigger()) :: :ok
end
