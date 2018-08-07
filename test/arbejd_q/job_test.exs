defmodule ArbejdQ.JobTest do
  use ArbejdQ.DataCase

  alias ArbejdQ.Job

  describe "ArbejdQ.Job.changeset/2:" do
    test "Minimum parameters" do
      changeset = Job.changeset(%Job{}, %{
        queue: "default",
        worker_module: "WorkerTest",
        parameters: %{a: 1, b: 3}
      })

      assert changeset.valid?
    end

    test "Missing :queue" do
      changeset = Job.changeset(%Job{}, %{
        worker_module: "WorkerTest",
        parameters: %{a: 1, b: 3}
      })

      refute changeset.valid?
    end

    test "Missing :worker_module" do
      changeset = Job.changeset(%Job{}, %{
        queue: "default",
        parameters: %{a: 1, b: 3}
      })

      refute changeset.valid?
    end

    test "Missing :parameters" do
      changeset = Job.changeset(%Job{}, %{
        queue: "default",
        worker_module: "WorkerTest",
      })

      refute changeset.valid?
    end
  end
end
