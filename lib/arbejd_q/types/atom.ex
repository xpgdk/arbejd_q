defmodule ArbejdQ.Types.Atom do
  @moduledoc """
  """

  use Ecto.Type

  def type, do: :string

  def cast(term), do: {:ok, term}

  def dump(atom), do: {:ok, Atom.to_string(atom)}

  def load(str), do: {:ok, String.to_existing_atom(str)}
end
