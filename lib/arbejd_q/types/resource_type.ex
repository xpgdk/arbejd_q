defmodule ArbejdQ.Types.ResourceType do
  @moduledoc """
  """

  use Ecto.Type

  @type t :: :ephemeral | :permanent

  @spec type :: :integer
  def type, do: :integer

  @spec cast(any) :: :error | {:ok, :ephemeral | :permanent}
  def cast(:ephemeral), do: {:ok, :ephemeral}
  def cast(:permanent), do: {:ok, :permanent}
  def cast("ephemeral"), do: {:ok, :ephemeral}
  def cast("permanent"), do: {:ok, :permanent}
  def cast(_), do: :error

  @spec dump(any) :: :error | {:ok, 0 | 1}
  def dump(:ephemeral), do: {:ok, 0}
  def dump(:permanent), do: {:ok, 1}
  def dump(_), do: :error

  @spec load(any) :: :error | {:ok, :ephemeral | :permanent}
  def load(0), do: {:ok, :ephemeral}
  def load(1), do: {:ok, :permanent}
  def load(_), do: :error
end
