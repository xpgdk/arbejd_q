defmodule ArbejdQ.Mixfile do
  use Mix.Project

  def project do
    [app: :arbejd_q,
     version: "3.1.0",
     elixir: "~> 1.12",
     elixirc_paths: elixirc_paths(Mix.env()),
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps(),
     dialyzer: [plt_add_apps: [:ecto]]]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Configuration for the OTP application
  #
  # Type "mix help compile.app" for more information
  def application do
    # Specify extra applications you'll use from Erlang/Elixir
    [extra_applications: [:logger, :crypto]]
  end

  # Dependencies can be Hex packages:
  #
  #   {:my_dep, "~> 0.3.0"}
  #
  # Or git/path repositories:
  #
  #   {:my_dep, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
  #
  # Type "mix help deps" for more examples and options
  defp deps do
    [
      {:ecto, "~> 3.7"},
      {:ecto_sql, "~> 3.7"},
      {:timex, "~> 3.7"},
      {:elixir_uuid, "~> 1.2"},
      {:typed_ecto_schema, "~> 0.4.0"},
      {:jason, "~> 1.2"},
      {:postgrex, "~> 0.15.10", only: :test},
      {:dialyxir, "~> 1.0.0-rc.3", only: :dev, runtime: false},
      {:ex_doc, "~> 0.25.2", only: :dev, runtime: false},
    ]
  end
end
