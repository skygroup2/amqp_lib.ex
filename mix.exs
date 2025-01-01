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
      {:skn_lib, git: "git@github.com:skygroup2/skn_lib.git", branch: "main"},
      {:amqp, "~> 4.0"},
      {:credo, "~> 1.7", only: [:dev], runtime: false},
      {:dialyxir, "~> 1.2", only: [:dev], runtime: false}
    ]
  end
end
