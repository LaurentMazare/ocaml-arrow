open Base

let create_t index =
  { Ppx_t.x = (index * 2) + 1
  ; y = Float.of_int index |> Float.sqrt
  ; z = Printf.sprintf "foo %d" index
  ; x_opt = (if index % 2 = 0 then None else Some index)
  ; y_opt = (if index % 3 = 0 then None else Some (Float.of_int index |> Float.exp))
  ; z_opt = (if index % 3 = 1 then None else Some (Printf.sprintf "%d-%d" index index))
  }

let%expect_test _ =
  let ts = Array.init 5 ~f:create_t in
  let table = Ppx_t.arrow_table_of_t ts in
  let table = Arrow_c_api.Table.concatenate [ table; table; table ] in
  let ts = Ppx_t.arrow_t_of_table table in
  Array.iter ts ~f:(fun t ->
      Ppx_t.sexp_of_t t |> Sexp.to_string_mach |> Stdio.printf "%s\n%!");
  [%expect
    {|
    ((x 1)(y 0)(z"foo 0")(x_opt())(y_opt())(z_opt(0-0)))
    ((x 3)(y 1)(z"foo 1")(x_opt(1))(y_opt(2.7182818284590451))(z_opt()))
    ((x 5)(y 1.4142135623730951)(z"foo 2")(x_opt())(y_opt(7.38905609893065))(z_opt(2-2)))
    ((x 7)(y 1.7320508075688772)(z"foo 3")(x_opt(3))(y_opt())(z_opt(3-3)))
    ((x 9)(y 2)(z"foo 4")(x_opt())(y_opt(54.598150033144236))(z_opt()))
    ((x 1)(y 0)(z"foo 0")(x_opt())(y_opt())(z_opt(0-0)))
    ((x 3)(y 1)(z"foo 1")(x_opt(1))(y_opt(2.7182818284590451))(z_opt()))
    ((x 5)(y 1.4142135623730951)(z"foo 2")(x_opt())(y_opt(7.38905609893065))(z_opt(2-2)))
    ((x 7)(y 1.7320508075688772)(z"foo 3")(x_opt(3))(y_opt())(z_opt(3-3)))
    ((x 9)(y 2)(z"foo 4")(x_opt())(y_opt(54.598150033144236))(z_opt()))
    ((x 1)(y 0)(z"foo 0")(x_opt())(y_opt())(z_opt(0-0)))
    ((x 3)(y 1)(z"foo 1")(x_opt(1))(y_opt(2.7182818284590451))(z_opt()))
    ((x 5)(y 1.4142135623730951)(z"foo 2")(x_opt())(y_opt(7.38905609893065))(z_opt(2-2)))
    ((x 7)(y 1.7320508075688772)(z"foo 3")(x_opt(3))(y_opt())(z_opt(3-3)))
    ((x 9)(y 2)(z"foo 4")(x_opt())(y_opt(54.598150033144236))(z_opt()))
      |}]
