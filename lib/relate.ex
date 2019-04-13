defmodule Relate do
  @moduledoc """

  Relate implements relational operators on Elixir enumerables.

  Join functions take two enumerables containing objects as well as one or
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

  ## Example
      iex> iso_countries = [
      ...>   {"us", "United States"},
      ...>   {"uk", "United Kingdom"},
      ...>   {"ca", "Canada"},
      ...>   {"de", "Germany"},
      ...>   {"nl", "Netherlands"},
      ...>   {"sg", "Singapore"},
      ...>   {"ru", "Russian Federation"},
      ...>   {"fr", "France"},
      ...>   {"ja", "Japan"},
      ...>   {"it", "Italy"},
      ...>   {"hk", "Hong Kong"},
      ...>   {"au", "Australia"},
      ...>   {"ch", "Switzerland"},
      ...>   {"be", "Belgium"},
      ...>   {"rk", "Korea, Republic of"},
      ...>   {"es", "Spain"},
      ...>   {"il", "Israel"}
      ...> ]
      ...>
      ...> country_clicks = [
      ...>   {"United States", "13"},
      ...>   {"United Kingdom", "11"},
      ...>   {"Canada", "4"},
      ...>   {"Germany", "4"},
      ...>   {"Netherlands", "3"},
      ...>   {"Singapore", "3"},
      ...>   {"Russian Federation", "2"},
      ...>   {"France", "2"},
      ...>   {"Japan", "2"},
      ...>   {"Italy", "2"},
      ...>   {"Hong Kong", "2"},
      ...>   {"Australia", "2"},
      ...>   {"Switzerland", "1"},
      ...>   {"Belgium", "1"},
      ...>   {"Korea, Republic of", "1"},
      ...>   {"Spain", "1"},
      ...>   {"Israel", "1"}
      ...> ]
      ...>
      ...> Relate.left_join(country_clicks, iso_countries, 0, 1)
      ...> |> Relate.select([right: 0, left: 1])
      ...> |> Enum.sort_by(&elem(&1, 0))
      [
        {"au", "2"},
        {"be", "1"},
        {"ca", "4"},
        {"ch", "1"},
        {"de", "4"},
        {"es", "1"},
        {"fr", "2"},
        {"hk", "2"},
        {"il", "1"},
        {"it", "2"},
        {"ja", "2"},
        {"nl", "3"},
        {"rk", "1"},
        {"ru", "2"},
        {"sg", "3"},
        {"uk", "11"},
        {"us", "13"}
      ]

  """

  @doc ~S"""

  Return an enumerable of tuples containing all elements of `e1` and
  `e2` which yield the same value when `fki1` and `fki2` are applied,
  respectively. If `fki2` is `nil` or `false`, `fki1` will be used to
  make comparisons on both enumerables.

  ## Examples

      iex> Relate.inner_join([%{k: 0, v: "zero"}, %{k: 1, v: "one"}],
      ...>                   [%{k: 1, v: "i"}, %{k: 2, v: "ii"}],
      ...>                   :k, :k)
      [{%{k: 1, v: "one"}, %{k: 1, v: "i"}}]

      iex> Relate.inner_join([%{k: 0, v: "zero"}, %{k: 1, v: "one"}],
      ...>                   [%{k: 1, v: "i"}, %{k: 2, v: "ii"}],
      ...>                   :k)  # NOTE: only one key function
      [{%{k: 1, v: "one"}, %{k: 1, v: "i"}}]

  """
  def inner_join(ds1, ds2, fki1, fki2 \\ nil) do
    {i1, i2} = indices(ds1, ds2, fki1, fki2)
    Enum.flat_map(intersection(Map.keys(i1), Map.keys(i2)), &rows(&1, i1, i2))
  end

  @doc """

  Return an enumerable of tuples containing all elements of `e1` and
  `e2` which yield the same value when `fki1` and `fki2` are applied,
  respectively. If `fki2` is `nil` or `false`, `fki1` will be used to
  make comparisons on both enumerables. Additionally, a tuple of the
  form `{nil, i}` or `{i, nil}` will be returned for every element of
  `e2` that did not match on any element in `e1` and vice-versa.

  ## Examples

      iex> Relate.outer_join([%{k: 0, v: "zero"}, %{k: 1, v: "one"}],
      ...>                   [%{k: 1, v: "i"}, %{k: 2, v: "ii"}],
      ...>                   :k, :k)
      [{%{k: 0, v: "zero"}, nil}, {%{k: 1, v: "one"}, %{k: 1, v: "i"}}, {nil, %{k: 2, v: "ii"}}]

      iex> Relate.outer_join([%{k: 0, v: "zero"}, %{k: 1, v: "one"}],
      ...>                   [%{k: 1, v: "i"}, %{k: 2, v: "ii"}],
      ...>                   :k)  # NOTE: only one key function
      ...> |> Enum.sort()
      [{nil, %{k: 2, v: "ii"}}, {%{k: 0, v: "zero"}, nil}, {%{k: 1, v: "one"}, %{k: 1, v: "i"}}]

  """
  def outer_join(ds1, ds2, fki1, fki2 \\ nil) do
    {i1, i2} = indices(ds1, ds2, fki1, fki2)
    Enum.flat_map(union(Map.keys(i1), Map.keys(i2)), &rows(&1, i1, i2))
  end

  @doc """

  Return an enumerable of tuples containing all elements of `e1` and
  `e2` which yield the same value when `fki1` and `fki2` are applied,
  respectively. If `fki2` is `nil` or `false`, `fki1` will be used to
  make comparisons on both enumerables. Additionally, a tuple of the
  form `{i, nil}` will be returned for every element of `e1` that did
  not match on any element in `e2`.

  ## Examples

      iex> Relate.left_join([%{k: 0, v: "zero"}, %{k: 1, v: "one"}],
      ...>                  [%{k: 1, v: "i"}, %{k: 2, v: "ii"}],
      ...>                  :k, :k)
      [{%{k: 0, v: "zero"}, nil}, {%{k: 1, v: "one"}, %{k: 1, v: "i"}}]

      iex> Relate.left_join([%{k: 0, v: "zero"}, %{k: 1, v: "one"}],
      ...>                  [%{k: 1, v: "i"}, %{k: 2, v: "ii"}],
      ...>                  :k)  # NOTE: only one key function
      [{%{k: 0, v: "zero"}, nil}, {%{k: 1, v: "one"}, %{k: 1, v: "i"}}]

  """
  def left_join(ds1, ds2, fki1, fki2 \\ nil) do
    {i1, i2} = indices(ds1, ds2, fki1, fki2)
    Enum.flat_map(Map.keys(i1), &rows(&1, i1, i2))
  end

  @doc """

  Return an enumerable of tuples containing all elements of `e1` and
  `e2` which yield the same value when `fki1` and `fki2` are applied,
  respectively. If `fki2` is `nil` or `false`, `fki1` will be used to
  make comparisons on both enumerables. Additionally, a tuple of the
  form `{nil, i}` will be returned for every element of `e2` that did
  not match on any element in `e1`.

  ## Examples

      iex> Relate.right_join([%{k: 0, v: "zero"}, %{k: 1, v: "one"}],
      ...>                   [%{k: 1, v: "i"}, %{k: 2, v: "ii"}],
      ...>                   :k, :k)
      [{%{k: 1, v: "one"}, %{k: 1, v: "i"}}, {nil, %{k: 2, v: "ii"}}]

      iex> Relate.right_join([%{k: 0, v: "zero"}, %{k: 1, v: "one"}],
      ...>                   [%{k: 1, v: "i"}, %{k: 2, v: "ii"}],
      ...>                   :k)  # NOTE: only one key function
      [{%{k: 1, v: "one"}, %{k: 1, v: "i"}}, {nil, %{k: 2, v: "ii"}}]

  """
  def right_join(ds1, ds2, fki1, fki2 \\ nil) do
    {i1, i2} = indices(ds1, ds2, fki1, fki2)
    Enum.flat_map(Map.keys(i2), &rows(&1, i1, i2))
  end

  defp rows(k, i1, i2) do
    for t1 <- Map.get(i1, k, MapSet.new([nil])),
        t2 <- Map.get(i2, k, MapSet.new([nil])),
        do: {t1, t2}
  end

  defp indices(ds1, ds2, fki1, fki2) do
    {make_index(ds1, to_f(fki1)), make_index(ds2, to_f(fki2 || fki1))}
  end

  defp make_index(ds, f) do
    Enum.map(ds, &{f.(&1), &1})
    |> Enum.reduce(
      %{},
      fn {k, v}, acc -> update_in(acc, [k], &set_put(&1, v)) end
    )
  end

  defp set_put(s, el), do: MapSet.put(to_set(s), el)
  defp to_set(nil), do: MapSet.new()
  defp to_set(x), do: MapSet.new(x)
  defp union(s1, s2), do: MapSet.union(MapSet.new(s1), MapSet.new(s2))

  defp intersection(s1, s2) do
    MapSet.intersection(MapSet.new(s1), MapSet.new(s2))
  end

  @doc ~S"""

  For each two element tuple in enumerable `join`, select each row
  specified by the `cols` keyword list. Each element `cols` should be
  a tuple with an initial element of `:left` or `:right` and a second
  element that acts as an accessor as in `inner_join/4` et al.

  ## Example

      iex> Relate.select([{{0, 1, 2}, {:a, :b, :c}},
      ...>                {{3, 4, 5}, {:d, :e, :f}}],
      ...>               [left: 0, right: 1, left: 2])
      [{0, :b, 2}, {3, :e, 5}]

  """
  def select(join, cols) do
    Enum.map(join, &(select1(&1, cols) |> List.to_tuple()))
  end

  defp select1(_t, []), do: []

  defp select1(t = {nil, _right}, [{:left, _} | rest]) do
    [nil | select1(t, rest)]
  end

  defp select1(t = {_left, nil}, [{:right, _} | rest]) do
    [nil | select1(t, rest)]
  end

  defp select1(t = {left, _right}, [{:left, fki} | rest]) do
    [to_f(fki).(left) | select1(t, rest)]
  end

  defp select1(t = {_left, right}, [{:right, fki} | rest]) do
    [to_f(fki).(right) | select1(t, rest)]
  end

  defp to_f(f) when is_function(f), do: f
  defp to_f(k) when is_atom(k), do: & &1[k]
  defp to_f(i) when is_integer(i) and i >= 0, do: &elem(&1, i)
end
