defmodule Relate.MixProject do
  use Mix.Project

  def project do
    [
      app: :relate,
      version: "0.3.0",
      elixir: "~> 1.8",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
      name: "Relate",
      source_url: "https://github.com/edw/elixir-relate"
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    []
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ex_doc, ">= 0.0.0", only: :dev}
    ]
  end

  defp description() do
    """
    Relate implements relational operators on Elixir enumerables
    """
  end

  defp package() do
    [
      name: "relate",
      licenses: ["Apache 2.0"],
      links: %{"GitHub" => "https://github.com/edw/elixir-relate"}
    ]
  end
end
