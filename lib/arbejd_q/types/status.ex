defmodule ArbejdQ.Types.Status do
  @moduledoc """
  """

  use Ecto.Type

  @type t :: :queued | :running | :done | :failed

  def type, do: :integer

  def cast(status) when is_atom(status) do
    case status do
      :queued -> {:ok, :queued}
      :running -> {:ok, :running}
      :done -> {:ok, :done}
      :failed -> {:ok, :failed}
      _ -> :error
    end
  end
  def cast(_), do: :error

  def dump(:queued), do: {:ok, 0}
  def dump(:running), do: {:ok, 1}
  def dump(:done), do: {:ok, 2}
  def dump(:failed), do: {:ok, 3}
  def dump(_), do: :error

  def load(0), do: {:ok, :queued}
  def load(1), do: {:ok, :running}
  def load(2), do: {:ok, :done}
  def load(3), do: {:ok, :failed}
  def load(_), do: :error
end
