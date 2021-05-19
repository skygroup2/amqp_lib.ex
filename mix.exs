defmodule AmqpEx.MixProject do
  use Mix.Project

  def project do
    [
      app: :amqp_ex,
      version: "0.1.0",
      elixir: "~> 1.11",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:skn_lib, git: "git@gitlab.com:gskynet_lib/skn_lib.git", branch: "master"},
      {:amqp, "~> 2.1"},
      {:credo, "~> 1.5", only: [:dev], runtime: false},
      {:dialyxir, "~> 1.1", only: [:dev], runtime: false}
    ]
  end
end
