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
