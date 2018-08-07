defmodule ArbejdQ.DataCase do
  use ExUnit.CaseTemplate

  using do
    quote do
      alias ArbejdQ.Test.Repo
    end
  end

  setup do
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(ArbejdQ.Test.Repo)
    :ok
  end
end
