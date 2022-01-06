open Core_kernel
open Arrow_c_api

let%expect_test _ =
  let col1 = Builder.String.create () in
  let col2 = Builder.Double.create () in
  let col3 = Builder.NativeInt.create () in
  let col4 = Builder.Int32.create () in
  for i = 1 to 3 do
    Builder.String.append col1 "v1";
    Builder.String.append col1 "v2";
    Builder.String.append col1 "v3";
    Builder.Double.append col2 (Float.of_int i +. 0.5);
    Builder.Double.append col2 (Float.of_int i +. 1.5);
    Builder.Double.append_opt col2 None;
    Builder.NativeInt.append_opt col3 (Some (2 * i));
    Builder.NativeInt.append_opt col3 None;
    Builder.NativeInt.append_opt col3 None;
    Builder.Int32.append_opt col4 (Some (Int32.of_int_exn (2 * i)));
    Builder.Int32.append_opt col4 (Some (Int32.of_int_exn (i * i)));
    Builder.Int32.append_opt col4 None
  done;
  let table =
    Builder.make_table
      [ "foo", String col1; "bar", Double col2; "baz", Int64 col3; "baz32", Int32 col4 ]
  in
  let foo = Wrapper.Column.read_utf8 table ~column:(`Name "foo") in
  let bar = Wrapper.Column.read_float_opt table ~column:(`Name "bar") in
  let baz = Wrapper.Column.read_int_opt table ~column:(`Name "baz") in
  let baz32 = Wrapper.Column.read_int32_opt table ~column:(`Name "baz32") in
  Array.iter foo ~f:(Stdio.printf "%s ");
  Stdio.printf "\n";
  Array.iter bar ~f:(fun v ->
      Option.value_map v ~f:Float.to_string ~default:"none" |> Stdio.printf "%s ");
  Stdio.printf "\n";
  Array.iter baz ~f:(fun v ->
      Option.value_map v ~f:Int.to_string ~default:"none" |> Stdio.printf "%s ");
  Stdio.printf "\n";
  Array.iter baz32 ~f:(fun v ->
      Option.value_map v ~f:Int32.to_string ~default:"none" |> Stdio.printf "%s ");
  Stdio.printf "\n";
  [%expect
    {|
    v1 v2 v3 v1 v2 v3 v1 v2 v3
    1.5 2.5 none 2.5 3.5 none 3.5 4.5 none
    2 none none 4 none none 6 none none
    2 1 none 4 4 none 6 9 none |}]

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

type t3 =
  { t : t
  ; time : Time_ns.t
  ; target : float option
  ; features : float array
  }
[@@deriving fields]

let t3_array_to_table =
  Fields_of_t3.to_list
    ~t:(Builder.C.c_flatten array_to_col_list)
    ~time:(Builder.C.c Time_ns)
    ~target:(Builder.C.c_opt Float)
    ~features:(Builder.C.c_array Float ~suffixes:[ "1s"; "5s"; "30s" ])
  |> List.concat
  |> Builder.C.array_to_table

let%expect_test _ =
  let t1 = { foo = 1337; bar = "fortytwo"; foobar = Some 0.57721566490153286 } in
  let t2 = { foo = 42; bar = "leet"; foobar = None } in
  let time = Time_ns.of_string "2020-01-16 15:15:00.000Z" in
  t3_array_to_table
    [| { t = t1; time; target = None; features = [| 1.; 2.; 3. |] }
     ; { t = t2; time; target = Some 3.14; features = [| 1.23; 2.34; 3.45 |] }
     ; { t = t1; time; target = Some 3.141; features = [| Float.nan; -1.11; 2.22 |] }
     ; { t = t2; time; target = Some 3.1415; features = [| 1.2345; -1.11; 2.22 |] }
     ; { t = t1; time; target = Some 3.14159; features = [| 2.71828; -1.11; 2.22 |] }
    |]
  |> Table.to_string_debug
  |> Stdio.print_endline;
  [%expect
    {|
    t_foo: int64 not null
    t_bar: string not null
    t_foobar: double
    time: timestamp[ns, tz=UTC] not null
    target: double
    features1s: double not null
    features5s: double not null
    features30s: double not null
    ----
    t_foo:
      [
        [
          1337,
          42,
          1337,
          42,
          1337
        ]
      ]
    t_bar:
      [
        [
          "fortytwo",
          "leet",
          "fortytwo",
          "leet",
          "fortytwo"
        ]
      ]
    t_foobar:
      [
        [
          0.577216,
          null,
          0.577216,
          null,
          0.577216
        ]
      ]
    time:
      [
        [
          2020-01-16 15:15:00.000000000,
          2020-01-16 15:15:00.000000000,
          2020-01-16 15:15:00.000000000,
          2020-01-16 15:15:00.000000000,
          2020-01-16 15:15:00.000000000
        ]
      ]
    target:
      [
        [
          null,
          3.14,
          3.141,
          3.1415,
          3.14159
        ]
      ]
    features1s:
      [
        [
          1,
          1.23,
          nan,
          1.2345,
          2.71828
        ]
      ]
    features5s:
      [
        [
          2,
          2.34,
          -1.11,
          -1.11,
          -1.11
        ]
      ]
    features30s:
      [
        [
          3,
          3.45,
          2.22,
          2.22,
          2.22
        ]
      ]

  |}]

type t4 =
  { t : t
  ; b1 : bool option
  ; b2 : bool
  ; b3 : bool
  }
[@@deriving fields]

let t4_array_to_table =
  Fields_of_t4.to_list
    ~t:(Builder.C.c_flatten array_to_col_list)
    ~b1:(Builder.C.c_opt Bool)
    ~b2:(Builder.C.c Bool)
    ~b3:(Builder.C.c Bool)
  |> List.concat
  |> Builder.C.array_to_table

let%expect_test _ =
  let t1 = { foo = 1337; bar = "fortytwo"; foobar = Some 0.57721566490153286 } in
  let t2 = { foo = 42; bar = "leet"; foobar = None } in
  t4_array_to_table
    [| { t = t1; b1 = None; b2 = true; b3 = false }
     ; { t = t2; b1 = None; b2 = true; b3 = false }
     ; { t = t1; b1 = Some true; b2 = true; b3 = false }
     ; { t = t2; b1 = Some false; b2 = true; b3 = false }
     ; { t = t1; b1 = None; b2 = true; b3 = true }
     ; { t = t1; b1 = Some true; b2 = true; b3 = true }
     ; { t = t1; b1 = Some true; b2 = true; b3 = true }
     ; { t = t1; b1 = None; b2 = true; b3 = true }
     ; { t = t1; b1 = None; b2 = false; b3 = false }
    |]
  |> Table.to_string_debug
  |> Stdio.print_endline;
  [%expect
    {|
    t_foo: int64 not null
    t_bar: string not null
    t_foobar: double
    b1: bool
    b2: bool not null
    b3: bool not null
    ----
    t_foo:
      [
        [
          1337,
          42,
          1337,
          42,
          1337,
          1337,
          1337,
          1337,
          1337
        ]
      ]
    t_bar:
      [
        [
          "fortytwo",
          "leet",
          "fortytwo",
          "leet",
          "fortytwo",
          "fortytwo",
          "fortytwo",
          "fortytwo",
          "fortytwo"
        ]
      ]
    t_foobar:
      [
        [
          0.577216,
          null,
          0.577216,
          null,
          0.577216,
          0.577216,
          0.577216,
          0.577216,
          0.577216
        ]
      ]
    b1:
      [
        [
          null,
          null,
          true,
          false,
          null,
          true,
          true,
          null,
          null
        ]
      ]
    b2:
      [
        [
          true,
          true,
          true,
          true,
          true,
          true,
          true,
          true,
          false
        ]
      ]
    b3:
      [
        [
          false,
          false,
          false,
          false,
          true,
          true,
          true,
          true,
          false
        ]
      ]

  |}]
