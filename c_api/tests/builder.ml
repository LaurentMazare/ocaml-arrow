open Core_kernel
open Arrow_c_api

let%expect_test _ =
  let col1 = Builder.String.create () in
  let col2 = Builder.Double.create () in
  let col3 = Builder.NativeInt.create () in
  for i = 1 to 3 do
    Builder.String.append col1 "v1";
    Builder.String.append col1 "v2";
    Builder.String.append col1 "v3";
    Builder.Double.append col2 (Float.of_int i +. 0.5);
    Builder.Double.append col2 (Float.of_int i +. 1.5);
    Builder.Double.append_opt col2 None;
    Builder.NativeInt.append_opt col3 (Some (2 * i));
    Builder.NativeInt.append_opt col3 None;
    Builder.NativeInt.append_opt col3 None
  done;
  let table =
    Builder.make_table [ "foo", String col1; "bar", Double col2; "baz", Int64 col3 ]
  in
  let foo = Wrapper.Column.read_utf8 table ~column:(`Name "foo") in
  let bar = Wrapper.Column.read_float_opt table ~column:(`Name "bar") in
  let baz = Wrapper.Column.read_int_opt table ~column:(`Name "baz") in
  Array.iter foo ~f:(Stdio.printf "%s ");
  Stdio.printf "\n";
  Array.iter bar ~f:(fun v ->
      Option.value_map v ~f:Float.to_string ~default:"none" |> Stdio.printf "%s ");
  Stdio.printf "\n";
  Array.iter baz ~f:(fun v ->
      Option.value_map v ~f:Int.to_string ~default:"none" |> Stdio.printf "%s ");
  Stdio.printf "\n";
  [%expect
    {|
    v1 v2 v3 v1 v2 v3 v1 v2 v3
    1.5 2.5 none 2.5 3.5 none 3.5 4.5 none
    2 none none 4 none none 6 none none |}]

type t =
  { foo : int
  ; bar : string
  ; foobar : float option
  }
[@@deriving fields, arrow, sexp]

let array_to_col_list =
  Fields.to_list
    ~foo:(Builder.C.c Int)
    ~bar:(Builder.C.c Utf8)
    ~foobar:(Builder.C.c_opt Float)
  |> List.concat

module RowBuilder = Builder.Row (struct
  type row = t

  let array_to_table = Builder.C.array_to_table array_to_col_list
end)

let%expect_test _ =
  let builder = RowBuilder.create () in
  RowBuilder.append builder { foo = 1; bar = "barbar"; foobar = None };
  RowBuilder.append builder { foo = 1337; bar = "pi"; foobar = Some 3.14169265358979 };
  let table = RowBuilder.to_table builder in
  arrow_t_of_table table
  |> Array.iter ~f:(fun t -> sexp_of_t t |> Sexp.to_string_mach |> Stdio.print_endline);
  [%expect
    {|
    ((foo 1)(bar barbar)(foobar()))
    ((foo 1337)(bar pi)(foobar(3.14169265358979)))
  |}];
  Table.to_string_debug table |> Stdio.print_endline;
  [%expect
    {|
    foo: int64 not null
    bar: string not null
    foobar: double
    ----
    foo:
      [
        [
          1,
          1337
        ]
      ]
    bar:
      [
        [
          "barbar",
          "pi"
        ]
      ]
    foobar:
      [
        [
          null,
          3.14169
        ]
      ]
  |}]

type t2 =
  { left : t
  ; right : t
  ; other : int
  }
[@@deriving sexp, fields]

let t2_array_to_table =
  Fields_of_t2.to_list
    ~left:(Builder.C.c_flatten array_to_col_list)
    ~right:(Builder.C.c_flatten array_to_col_list)
    ~other:(Builder.C.c Int)
  |> List.concat
  |> Builder.C.array_to_table

let%expect_test _ =
  let left = { foo = 123456; bar = "onetwothree"; foobar = None } in
  let right = { foo = -2992792458; bar = "ccc"; foobar = Some 2.71828182846 } in
  t2_array_to_table
    [| { left; right; other = 42 }
     ; { left = right; right = left; other = -42 }
     ; { left; right = left; other = 1337 }
    |]
  |> Table.to_string_debug
  |> Stdio.print_endline;
  [%expect
    {|
    left_foo: int64 not null
    left_bar: string not null
    left_foobar: double
    right_foo: int64 not null
    right_bar: string not null
    right_foobar: double
    other: int64 not null
    ----
    left_foo:
      [
        [
          123456,
          -2992792458,
          123456
        ]
      ]
    left_bar:
      [
        [
          "onetwothree",
          "ccc",
          "onetwothree"
        ]
      ]
    left_foobar:
      [
        [
          null,
          2.71828,
          null
        ]
      ]
    right_foo:
      [
        [
          -2992792458,
          123456,
          123456
        ]
      ]
    right_bar:
      [
        [
          "ccc",
          "onetwothree",
          "onetwothree"
        ]
      ]
    right_foobar:
      [
        [
          2.71828,
          null,
          null
        ]
      ]
    other:
      [
        [
          42,
          -42,
          1337
        ]
      ] |}]
