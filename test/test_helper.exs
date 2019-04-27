ExUnit.start

ArbejdQ.Test.Repo.start_link()
Ecto.Migrator.run(ArbejdQ.Test.Repo, "test/support/migrations/", :up, [all: true])

Ecto.Adapters.SQL.Sandbox.mode(ArbejdQ.Test.Repo, :manual)
