defmodule ArbejdQ.Resource do
  use TypedEctoSchema
  import Ecto.Changeset

  alias ArbejdQ.Types.ResourceType

  @primary_key {:id, :string, autogenerate: false}
  typed_schema "arbejdq_resources" do
    field(:type, ResourceType, null: false, enforce: true)
    field(:total_count, :integer, null: false, enforce: true)
    field(:available_count, :integer, null: false, enforce: true)

    timestamps()
  end

  @type changeset :: Ecto.Changeset.t(t())

  @spec changeset(changeset() | t(), map()) :: changeset()
  def changeset(struct, params) do
    struct
    |> cast(params, [:total_count, :available_count])
    |> unique_constraint(:id, name: :arbejdq_resources_pkey)
  end

  # @spec build(ArbejdQ.Job.t(), String.t()) :: changeset()
  # def build(%ArbejdQ.Job{id: job_id}, resource_id) do
  #   %__MODULE__{}
  #   |> change()
  #   |> put_change(:resource_id, resource_id)
  #   |> put_change(:job_id, job_id)
  # end

  @spec build_ephemeral(String.t()) :: changeset()
  def build_ephemeral(resource_id) do
    %__MODULE__{
      id: resource_id,
      type: :ephemeral,
      total_count: 1,
      available_count: 0
    }
    |> changeset(%{})
  end
end
