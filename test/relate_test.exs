defmodule RelateTest do
  use ExUnit.Case
  doctest Relate

  setup do
    {
      :ok,
      english: [
        {1, "one"},
        {0, "zero"},
        {2, "two"},
        {3, "three"},
        {4, "four"},
        {5, "five"},
        {6, "six"},
        {7, "seven"}
      ],
      roman: [
        %{n: 1, str: "i"},
        %{n: 2, str: "ii"},
        %{n: 3, str: "iii"},
        %{n: 4, str: "iiii"},
        %{n: 4, str: "iv"},
        %{n: 5, str: "v"},
        %{n: 6, str: "vi"},
        %{n: 7, str: "vii"},
        %{n: 8, str: "viii"},
        %{n: 9, str: "viiii"},
        %{n: 9, str: "ix"},
        %{n: 10, str: "x"}
      ]
    }
  end

  test "outer_join works against a tuple and map enums",
       %{english: english, roman: roman} do
    assert Relate.outer_join(english, roman, 0, :n) |> Enum.sort() == [
             {nil, %{n: 8, str: "viii"}},
             {nil, %{n: 9, str: "ix"}},
             {nil, %{n: 9, str: "viiii"}},
             {nil, %{n: 10, str: "x"}},
             {{0, "zero"}, nil},
             {{1, "one"}, %{n: 1, str: "i"}},
             {{2, "two"}, %{n: 2, str: "ii"}},
             {{3, "three"}, %{n: 3, str: "iii"}},
             {{4, "four"}, %{n: 4, str: "iiii"}},
             {{4, "four"}, %{n: 4, str: "iv"}},
             {{5, "five"}, %{n: 5, str: "v"}},
             {{6, "six"}, %{n: 6, str: "vi"}},
             {{7, "seven"}, %{n: 7, str: "vii"}}
           ]
  end
end
