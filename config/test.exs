import Config
#
# Print only warnings and errors during test
config :logger, level: :warn

config :arbejd_q,
  ecto_repos: [ArbejdQ.Test.Repo],
  repo: ArbejdQ.Test.Repo,
  stale_job_period: 5,
  default_expiration_duration: 5,
  default_scheduler_name: :arbejd_q_scheduler


config :arbejd_q, ArbejdQ.Test.Repo,
  adapter: Ecto.Adapters.Postgres,
  username: "postgres",
  password: "postgres",
  database: "arbejd_q_test",
  hostname: "localhost",
  pool: Ecto.Adapters.SQL.Sandbox,
  priv: "test/support/"
