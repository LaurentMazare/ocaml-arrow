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
      |}];
  let col = Arrow_c_api.Table.read table ~column:(`Name "y") Float in
  Stdio.printf "%s\n%!" ([%sexp_of: float array] col |> Sexp.to_string);
  let col = Arrow_c_api.Table.read_opt table ~column:(`Name "y") Float in
  Stdio.printf "%s\n%!" ([%sexp_of: float option array] col |> Sexp.to_string);
  [%expect
    {|
    (0 1 1.4142135623730951 1.7320508075688772 2 0 1 1.4142135623730951 1.7320508075688772 2 0 1 1.4142135623730951 1.7320508075688772 2)
    ((0)(1)(1.4142135623730951)(1.7320508075688772)(2)(0)(1)(1.4142135623730951)(1.7320508075688772)(2)(0)(1)(1.4142135623730951)(1.7320508075688772)(2)) |}]

let create_t2 index t s o =
  let t = Core_kernel.Time_ns.of_string t in
  let s = Core_kernel.Time_ns.Span.of_string s in
  let o = Core_kernel.Time_ns.Ofday.of_string o in
  { Ppx_t.x = (index * 2) + 1
  ; t
  ; t_opt = (if index % 2 = 0 then Some t else None)
  ; s
  ; s_opt = (if index % 3 = 0 then Some s else None)
  ; o
  ; o_opt = (if index % 3 = 1 then Some o else None)
  }

let%expect_test _ =
  let ts =
    [| create_t2 0 "2021-01-01 15:15:00.123456789Z" "1s" "12:35:12.000111222"
     ; create_t2 1 "2021-01-01 15:15:00Z" "-11s11ns" "00:35"
     ; create_t2 2 "2021-01-01 15:15:00Z" "-11s11ns" "00:35"
     ; create_t2 3 "2021-01-01 15:15:00Z" "0s" "00:35:00.123"
     ; create_t2 4 "2021-01-01 15:15:00Z" "11d11ns" "00:35"
     ; create_t2 5 "2021-01-01 00:15:00Z" "-11d11ns" "00:35"
     ; create_t2 6 "2010-01-01 05:15:00.123Z" "-11s11ns" "00:35"
     ; create_t2 7 "2020-01-01 15:15:00.123456Z" "-11s11ns" "00:35"
    |]
  in
  let table = Ppx_t.arrow_table_of_t2 ts in
  let table = Arrow_c_api.Table.concatenate [ table; table; table ] in
  let ts2 = Ppx_t.arrow_t2_of_table table in
  assert (Caml.( = ) (Array.concat [ ts; ts; ts ]) ts2)
