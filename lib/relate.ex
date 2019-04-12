defmodule Relate do
  @moduledoc """

  Relate implements relational operators on Elixir enumerables.

  Functions take two enumerables containing objects as well as one or
  two arguments to specify how the value to use as a basis for joining
  will be determined.

  If either `fki1` or `fki2` are functions, the function will be
  called with the object as an argument on `e1` or `e2`,
  respectively. If the value is an atom, the object will be treated as
  a map and the join value will be the value of the key associated
  with the atom. If the value is a non-negative integer, the object
  will be treated as a tuple and the value will be treated as an index
  into it.

  If `fki2` is not specified or is `nil` or `false`, the same accessor
  will be used on both `e1` and `e2`.

  """

  @doc ~S"""

  Return an enumerable of tuples containing all elements of `e1` and
  `e2` which yield the same value when `fki1` and `fki2` are applied,
  respectively. If `fki2` is `nil` or `false`, `fki1` will be used to
  make comparisons on both enumerables.

  ### Examples

    iex> Relate.inner_join([%{k: 0, v: "zero"}, %{k: 1, v: "one"}],
    ...>                   [%{k: 1, v: "i"}, %{k: 2, v: "ii"}],
    ...>                   :k, :k)
    [{%{k: 1, v: "one"}, %{k: 1, v: "i"}}]

    iex> Relate.inner_join([%{k: 0, v: "zero"}, %{k: 1, v: "one"}],
    ...>                   [%{k: 1, v: "i"}, %{k: 2, v: "ii"}],
    ...>                   :k)  # NOTE: only one key function
    [{%{k: 1, v: "one"}, %{k: 1, v: "i"}}]

  """
  def inner_join(e1, e2, fki1, fki2 \\ nil) do
    f1 = to_f(fki1)
    f2 = to_f(fki2 || f1)

    for x1 <- e1,
        x2 <- e2,
        f1.(x1) == f2.(x2) do
      {x1, x2}
    end
  end

  @doc """

  Return an enumerable of tuples containing all elements of `e1` and
  `e2` which yield the same value when `fki1` and `fki2` are applied,
  respectively. If `fki2` is `nil` or `false`, `fki1` will be used to
  make comparisons on both enumerables. Additionally, a tuple of the
  form `{i, nil}` will be returned for every element of `e1` that did
  not match on any element in `e2`.

  ### Examples

    iex> Relate.left_join([%{k: 0, v: "zero"}, %{k: 1, v: "one"}],
    ...>                  [%{k: 1, v: "i"}, %{k: 2, v: "ii"}],
    ...>                  :k, :k)
    [{%{k: 0, v: "zero"}, nil}, {%{k: 1, v: "one"}, %{k: 1, v: "i"}}]

    iex> Relate.left_join([%{k: 0, v: "zero"}, %{k: 1, v: "one"}],
    ...>                  [%{k: 1, v: "i"}, %{k: 2, v: "ii"}],
    ...>                  :k)  # NOTE: only one key function
    [{%{k: 0, v: "zero"}, nil}, {%{k: 1, v: "one"}, %{k: 1, v: "i"}}]

  """
  def left_join(e1, e2, fki1, fki2 \\ nil) do
    f1 = to_f(fki1)
    f2 = to_f(fki2 || f1)

    for x <- e1, reduce: [] do
      acc ->
        Enum.concat(acc, left_join_helper(x, e2, f1, f2))
    end
  end

  defp left_join_helper(x, e, f1, f2) do
    Enum.reduce(
      e,
      [{x, nil}],
      fn
        y, acc = [{^x, nil}] ->
          if f1.(x) == f2.(y), do: [{x, y}], else: acc

        y, acc ->
          if f1.(x) == f2.(y), do: [{x, y} | acc], else: acc
      end
    )
  end

  @doc """

  Return an enumerable of tuples containing all elements of `e1` and
  `e2` which yield the same value when `fki1` and `fki2` are applied,
  respectively. If `fki2` is `nil` or `false`, `fki1` will be used to
  make comparisons on both enumerables. Additionally, a tuple of the
  form `{nil, i}` will be returned for every element of `e2` that did
  not match on any element in `e1`.

  ### Examples

    iex> Relate.right_join([%{k: 0, v: "zero"}, %{k: 1, v: "one"}],
    ...>                   [%{k: 1, v: "i"}, %{k: 2, v: "ii"}],
    ...>                   :k, :k)
    [{%{k: 1, v: "one"}, %{k: 1, v: "i"}}, {nil, %{k: 2, v: "ii"}}]

    iex> Relate.right_join([%{k: 0, v: "zero"}, %{k: 1, v: "one"}],
    ...>                   [%{k: 1, v: "i"}, %{k: 2, v: "ii"}],
    ...>                   :k)  # NOTE: only one key function
    [{%{k: 1, v: "one"}, %{k: 1, v: "i"}}, {nil, %{k: 2, v: "ii"}}]

  """
  def right_join(e1, e2, fki1, fki2 \\ nil) do
    f1 = to_f(fki1)
    f2 = to_f(fki2 || f1)

    for x <- e2, reduce: [] do
      acc ->
        Enum.concat(acc, right_join_helper(x, e1, f1, f2))
    end
  end

  defp right_join_helper(x, e, f1, f2) do
    Enum.reduce(
      e,
      [{nil, x}],
      fn
        y, acc = [{nil, ^x}] ->
          if f2.(x) == f1.(y), do: [{y, x}], else: acc

        y, acc ->
          if f2.(x) == f1.(y), do: [{y, x} | acc], else: acc
      end
    )
  end

  @doc """

  Return an enumerable of tuples containing all elements of `e1` and
  `e2` which yield the same value when `fki1` and `fki2` are applied,
  respectively. If `fki2` is `nil` or `false`, `fki1` will be used to
  make comparisons on both enumerables. Additionally, a tuple of the
  form `{nil, i}` or `{i, nil}` will be returned for every element of
  `e2` that did not match on any element in `e1` and vice-versa.

  ### Examples

    iex> Relate.outer_join([%{k: 0, v: "zero"}, %{k: 1, v: "one"}],
    ...>                   [%{k: 1, v: "i"}, %{k: 2, v: "ii"}],
    ...>                   :k, :k)
    [{%{k: 0, v: "zero"}, nil}, {%{k: 1, v: "one"}, %{k: 1, v: "i"}}, {nil, %{k: 2, v: "ii"}}]

    iex> Relate.outer_join([%{k: 0, v: "zero"}, %{k: 1, v: "one"}],
    ...>                   [%{k: 1, v: "i"}, %{k: 2, v: "ii"}],
    ...>                   :k)  # NOTE: only one key function
    [{%{k: 0, v: "zero"}, nil}, {%{k: 1, v: "one"}, %{k: 1, v: "i"}}, {nil, %{k: 2, v: "ii"}}]

  """
  def outer_join(e1, e2, fki1, fki2 \\ nil) do
    Enum.concat(left_join(e1, e2, fki1, fki2), right_join(e1, e2, fki1, fki2))
    |> Enum.uniq()
  end

  defp to_f(f) when is_function(f), do: f
  defp to_f(k) when is_atom(k), do: & &1[k]
  defp to_f(i) when is_integer(i) and i >= 0, do: &elem(&1, i)
end
