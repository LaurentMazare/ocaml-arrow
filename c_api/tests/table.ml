open Core_kernel
open Arrow_c_api

let%expect_test _ =
  let table =
    List.init 3 ~f:(fun i ->
        let cols =
          [ Wrapper.Writer.utf8 [| "v1"; "v2"; "v3" |] ~name:"foo"
          ; Wrapper.Writer.int [| i; 5 * i; 10 * i |] ~name:"bar"
          ; Wrapper.Writer.int_opt [| Some ((i * 2) + 1); None; None |] ~name:"baz"
          ]
        in
        Wrapper.Writer.create_table ~cols)
    |> Wrapper.Table.concatenate
  in
  let foo = Wrapper.Column.read_utf8 table ~column:(`Name "foo") in
  let bar = Wrapper.Column.read_int table ~column:(`Name "bar") in
  let baz = Wrapper.Column.read_int_opt table ~column:(`Name "baz") in
  Array.iter foo ~f:(Stdio.printf "%s ");
  Stdio.printf "\n";
  Array.iter bar ~f:(Stdio.printf "%d ");
  Stdio.printf "\n";
  Array.iter baz ~f:(fun v ->
      Option.value_map v ~f:Int.to_string ~default:"none" |> Stdio.printf "%s ");
  Stdio.printf "\n";
  [%expect
    {|
    v1 v2 v3 v1 v2 v3 v1 v2 v3
    0 0 0 1 5 10 2 10 20
    1 none none 3 none none 5 none none |}]

let%expect_test _ =
  let table =
    List.init 3 ~f:(fun i ->
        let f = Float.of_int i in
        let cols =
          [ Table.col [| "v1"; "v2"; "v3" |] Utf8 ~name:"foo"
          ; Table.col [| f; 5. *. f; 0.5 *. f |] Float ~name:"bar"
          ; Table.col_opt [| Some ((f *. 2.) +. 0.1); None; None |] Float ~name:"baz"
          ]
        in
        Wrapper.Writer.create_table ~cols)
    |> Wrapper.Table.concatenate
  in
  let foo = Table.read table Utf8 ~column:(`Name "foo") in
  let bar = Table.read table Float ~column:(`Name "bar") in
  let baz = Table.read_opt table Float ~column:(`Name "baz") in
  Array.iter foo ~f:(Stdio.printf "%s ");
  Stdio.printf "\n";
  Array.iter bar ~f:(Stdio.printf "%f ");
  Stdio.printf "\n";
  Array.iter baz ~f:(fun v ->
      Option.value_map v ~f:Float.to_string ~default:"none" |> Stdio.printf "%s ");
  Stdio.printf "\n";
  [%expect
    {|
    v1 v2 v3 v1 v2 v3 v1 v2 v3
    0.000000 0.000000 0.000000 1.000000 5.000000 0.500000 2.000000 10.000000 1.000000
    0.1 none none 2.1 none none 4.1 none none |}]
