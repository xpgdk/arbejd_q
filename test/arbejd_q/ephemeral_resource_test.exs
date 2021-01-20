defmodule ArbejdQ.EphemeralResourceTest do
  use ArbejdQ.DataCase

  alias ArbejdQ.{
    Scheduler,
    ResourceRequirement,
    Resource
  }

  setup do
    Ecto.Adapters.SQL.Sandbox.mode(ArbejdQ.Test.Repo, {:shared, self()})
  end

  defp initial_scheduler_state() do
    %{
      workers: [],
      poll_interval: 10_000,
      max_jobs: 3,
      queues: [
        high: [
          max_jobs: 1,
          priority: 10
        ],
        normal: [
          max_jobs: 2,
          priority: 1
        ]
      ],
      disable_timer: true,
      disable_execution: false,
      timer_ref: nil,
      last_time_of_poll: nil,
      last_time_of_job_refresh: nil,
      last_time_of_stale_reset: nil
    }
  end

  defp two_schedulers(tags) do
    scheduler_1_state = initial_scheduler_state()
    scheduler_2_state = initial_scheduler_state()

    Map.merge(tags, %{
      scheduler_1_state: scheduler_1_state,
      scheduler_2_state: scheduler_2_state
    })
  end

  describe "Two schedulers" do
    setup [:two_schedulers]

    test "Ephemeral resource are automatically created and removed", tags do
      resources = [
        %ResourceRequirement{
          id: "1234",
          count: 1,
          type: :ephemeral
        }
      ]

      assert {:ok, job_1} =
               ArbejdQ.create_job("normal", ArbejdQ.Test.Worker, %{duration: 1},
                 resources: resources
               )

      tags.scheduler_1_state
      |> Scheduler.schedule_jobs()

      assert [
               %Resource{
                 type: :ephemeral,
                 id: "1234",
                 total_count: 1,
                 available_count: 0
               }
             ] = ArbejdQ.list_resources()

      ArbejdQ.wait(job_1)
      assert [] = ArbejdQ.list_resources()
    end

    test "Resource are freed, when job raises", tags do
      resources = [
        %ResourceRequirement{
          id: "1234",
          count: 1,
          type: :ephemeral
        }
      ]

      assert {:ok, job_1} =
               ArbejdQ.create_job("normal", ArbejdQ.Test.Worker, %{raise: true, duration: 1},
                 resources: resources
               )

      tags.scheduler_1_state
      |> Scheduler.schedule_jobs()

      ArbejdQ.wait(job_1)
      assert [] = ArbejdQ.list_resources()
    end

    test "Resources are freed, when stale job are released", tags do
      resources = [
        %ResourceRequirement{
          id: "1234",
          count: 1,
          type: :ephemeral
        }
      ]

      assert {:ok, job_1} =
               ArbejdQ.create_job("normal", ArbejdQ.Test.Worker, %{duration: 5},
                 resources: resources
               )

      pid =
        spawn(fn ->
          tags.scheduler_1_state
          |> Scheduler.schedule_jobs()
        end)

      Process.sleep(2000)

      Process.exit(pid, :kill)

      assert {:ok, job_1} = ArbejdQ.get_job(job_1)
      assert job_1.status == :running

      ArbejdQ.Job.changeset(job_1, %{
        status_updated: Timex.subtract(Timex.now(), Timex.Duration.from_hours(1))
      })
      |> ArbejdQ.repo().update()

      Scheduler.handle_info(:handle_timer, tags.scheduler_1_state)

      assert {:ok, job_1} = ArbejdQ.get_job(job_1)
      assert job_1.status == :queued
      assert job_1.stale_counter == 1
      assert [] = ArbejdQ.list_resources()
    end

    test "Concurrent release of stale jobs", tags do
      resources = [
        %ResourceRequirement{
          id: "1234",
          count: 1,
          type: :ephemeral
        }
      ]

      assert {:ok, job_1} =
               ArbejdQ.create_job("normal", ArbejdQ.Test.Worker, %{duration: 5},
                 resources: resources
               )

      pid =
        spawn(fn ->
          tags.scheduler_1_state
          |> Scheduler.schedule_jobs()
        end)

      Process.sleep(2000)

      Process.exit(pid, :kill)

      assert {:ok, job_1} = ArbejdQ.get_job(job_1)
      assert job_1.status == :running

      ArbejdQ.Job.changeset(job_1, %{
        status_updated: Timex.subtract(Timex.now(), Timex.Duration.from_hours(1))
      })
      |> ArbejdQ.repo().update()

      t1 =
        Task.async(fn ->
          Scheduler.handle_info(:handle_timer, tags.scheduler_1_state)
        end)

      t2 =
        Task.async(fn ->
          Scheduler.handle_info(:handle_timer, tags.scheduler_1_state)
        end)

      Task.await(t1)
      Task.await(t2)

      assert {:ok, job_1} = ArbejdQ.get_job(job_1)
      assert job_1.status == :queued
      assert job_1.stale_counter == 1
      assert [] = ArbejdQ.list_resources()
    end

    test "Two concurrent schedulers", tags do
      resources = [
        %ResourceRequirement{
          id: "1234",
          count: 1,
          type: :ephemeral
        }
      ]

      assert {:ok, job_1} =
               ArbejdQ.create_job("normal", ArbejdQ.Test.Worker, %{duration: 1},
                 resources: resources
               )

      assert {:ok, job_2} =
               ArbejdQ.create_job("normal", ArbejdQ.Test.Worker, %{duration: 1},
                 resources: resources
               )

      spawn(fn ->
        tags.scheduler_1_state
        |> Scheduler.schedule_jobs()
      end)

      spawn(fn ->
        tags.scheduler_2_state
        |> Scheduler.schedule_jobs()
      end)

      ArbejdQ.wait(job_1)
      assert {:ok, job_2} = ArbejdQ.get_job(job_2)
      assert job_2.status == :queued
    end

    test "Jobs with non-overlapping resource requirements are run concurrently", tags do
      resources_1 = [
        %ResourceRequirement{
          id: "1234",
          count: 1,
          type: :ephemeral
        }
      ]

      resources_2 = [
        %ResourceRequirement{
          id: "abcdef",
          count: 1,
          type: :ephemeral
        }
      ]

      assert {:ok, job_1} =
               ArbejdQ.create_job("normal", ArbejdQ.Test.Worker, %{duration: 1},
                 resources: resources_1
               )

      assert {:ok, job_2} =
               ArbejdQ.create_job("normal", ArbejdQ.Test.Worker, %{duration: 1},
                 resources: resources_2
               )

      tags.scheduler_1_state
      |> Scheduler.schedule_jobs()

      assert {:ok, job_1} = ArbejdQ.get_job(job_1)
      assert job_1.status == :running

      assert {:ok, job_2} = ArbejdQ.get_job(job_2)
      assert job_2.status == :running

      ArbejdQ.wait(job_1)
      ArbejdQ.wait(job_2)
    end
  end
end
