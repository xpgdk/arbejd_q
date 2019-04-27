defmodule ArbejdQ.Test.Repo do
  use Ecto.Repo,
    otp_app: :arbejd_q,
    adapter: Ecto.Adapters.Postgres
end
