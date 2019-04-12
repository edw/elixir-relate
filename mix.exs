defmodule Query.MixProject do
  use Mix.Project

  def project do
    [
      app: :query,
      version: "0.1.0",
      elixir: "~> 1.8",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package()
    ]
  end

    # Run "mix help compile.app" to learn about applications.
  def application do
    []
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    []
  end

  defp description() do
    """
    Perform relational database joins on Elixir enumerables.
    """
  end

  defp package() do
    [
      name: "query",
      licenses: ["Apache 2.0"],
      links: %{"GitHub" => "https://github.com/edw/elixir-query"}
  end
end
