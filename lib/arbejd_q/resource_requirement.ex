defmodule ArbejdQ.ResourceRequirement do
  use TypedEctoSchema
  import Ecto.Changeset, only: [cast: 3]

  @primary_key false
  typed_embedded_schema do
    field(:id, :string, primary_key: true, null: false, enforce: true)
    field(:count, :integer, default: 1, null: false)
    field(:type, ArbejdQ.Types.ResourceType, null: false, enforce: true)
  end

  @type changeset :: Ecto.Changeset.t(t())

  @spec changeset(changeset() | t(), map()) :: changeset()
  def changeset(struct, params) do
    struct
    |> cast(params, [:id, :count, :type])
  end
end
