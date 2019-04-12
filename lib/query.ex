defmodule Query do
  @moduledoc """
  Query implements relational operators on Elixir enumerables. It
  currently supports four types of joins: inner, left, right, and
  outer.
  """

  def inner_join(e1, e2, f_or_k), do: inner_join(e1, e2, f_or_k, f_or_k)

  def inner_join(e1, e2, k1, k2) when is_atom(k1) and is_atom(k2) do
    inner_join(e1, e2, & &1[k1], & &1[k2])
  end

  def inner_join(e1, e2, k1, k2) when is_integer(k1) and is_atom(k2) do
    inner_join(e1, e2, &elem(&1, k1), & &1[k2])
  end

  def inner_join(e1, e2, k1, k2) when is_atom(k1) and is_integer(k2) do
    inner_join(e1, e2, & &1[k1], &elem(&1, k2))
  end

  def inner_join(e1, e2, k1, k2) when is_integer(k1) and is_integer(k2) do
    inner_join(e1, e2, &elem(&1, k1), &elem(&1, k2))
  end

  def inner_join(e1, e2, f1, f2) do
    for x1 <- e1,
        x2 <- e2,
        f1.(x1) == f2.(x2) do
      {x1, x2}
    end
  end

  def left_join(e1, e2, f_or_k), do: left_join(e1, e2, f_or_k, f_or_k)

  def left_join(e1, e2, k1, k2) when is_atom(k1) and is_atom(k2) do
    left_join(e1, e2, & &1[k1], & &1[k2])
  end

  def left_join(e1, e2, k1, k2) when is_integer(k1) and is_atom(k2) do
    left_join(e1, e2, &elem(&1, k1), & &1[k2])
  end

  def left_join(e1, e2, k1, k2) when is_atom(k1) and is_integer(k2) do
    left_join(e1, e2, & &1[k1], &elem(&1, k2))
  end

  def left_join(e1, e2, k1, k2) when is_integer(k1) and is_integer(k2) do
    left_join(e1, e2, &elem(&1, k1), &elem(&1, k2))
  end

  def left_join(e1, e2, f1, f2) do
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

  def right_join(e1, e2, f_or_k), do: right_join(e1, e2, f_or_k, f_or_k)

  def right_join(e1, e2, k1, k2) when is_atom(k1) and is_atom(k2) do
    right_join(e1, e2, & &1[k1], & &1[k2])
  end

  def right_join(e1, e2, k1, k2) when is_integer(k1) and is_atom(k2) do
    right_join(e1, e2, &elem(&1, k1), & &1[k2])
  end

  def right_join(e1, e2, k1, k2) when is_atom(k1) and is_integer(k2) do
    right_join(e1, e2, & &1[k1], &elem(&1, k2))
  end

  def right_join(e1, e2, k1, k2) when is_integer(k1) and is_integer(k2) do
    right_join(e1, e2, &elem(&1, k1), &elem(&1, k2))
  end

  def right_join(e1, e2, f1, f2) do
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

  def outer_join(e1, e2, f_or_k), do: outer_join(e1, e2, f_or_k, f_or_k)

  def outer_join(e1, e2, k1, k2) when is_atom(k1) and is_atom(k2) do
    outer_join(e1, e2, & &1[k1], & &1[k2])
  end

  def outer_join(e1, e2, k1, k2) when is_integer(k1) and is_atom(k2) do
    outer_join(e1, e2, &elem(&1, k1), & &1[k2])
  end

  def outer_join(e1, e2, k1, k2) when is_atom(k1) and is_integer(k2) do
    outer_join(e1, e2, & &1[k1], &elem(&1, k2))
  end

  def outer_join(e1, e2, k1, k2) when is_integer(k1) and is_integer(k2) do
    outer_join(e1, e2, &elem(&1, k1), &elem(&1, k2))
  end

  def outer_join(e1, e2, f1, f2) do
    Enum.concat(left_join(e1, e2, f1, f2), right_join(e1, e2, f1, f2))
    |> Enum.uniq()
  end
end
