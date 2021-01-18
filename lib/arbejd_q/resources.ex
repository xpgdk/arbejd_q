defmodule ArbejdQ.Resources do
  @moduledoc """
  Resource handling.
  """
  alias ArbejdQ.{Job, Resource, ResourceRequirement}

  @doc """
  Acquire resources required to run `job`.

  It is important that this is run in a transaction that also allocates the job.
  """
  @spec acquire_resources(Job.t()) :: :ok
  def acquire_resources(%Job{resource_requirements: []}), do: :ok

  def acquire_resources(%Job{resource_requirements: resources}) when is_list(resources) do
    Enum.each(resources, &acquire_resource/1)
  end

  @spec acquire_resource(ResourceRequirement.t()) :: :ok
  defp acquire_resource(%ResourceRequirement{type: :ephemeral, id: resource_id}) do
    # Ephemeral resources are created allocated.
    # As their ID is the primary key, insertion will fail if it exists already.
    Resource.build_ephemeral(resource_id)
    |> ArbejdQ.repo().insert!()

    :ok
  end

  @doc """
  Free resources for a job.

  It is important that this is run in a transaction that frees the job as well.
  """
  @spec free_resources(Job.t()) :: :ok
  def free_resources(%Job{resource_requirements: []}), do: :ok

  def free_resources(%Job{resource_requirements: resource_requirements}) do
    Enum.each(resource_requirements, &free_resource/1)
  end

  @spec free_resource(ResourceRequirement.t()) :: :ok
  defp free_resource(%ResourceRequirement{type: :ephemeral, id: resource_id}) do
    Resource.build_ephemeral(resource_id)
    |> ArbejdQ.repo().delete!()
  end

end
