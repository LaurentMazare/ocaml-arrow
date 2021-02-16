open Core_kernel
open Arrow_c_api

let%expect_test _ =
  let t =
    List.init 12 ~f:(fun i ->
        let f = Float.of_int i in
        let cols =
          [ Wrapper.Writer.utf8 [| "v1"; "v2"; "v3" |] ~name:"foo"
          ; Wrapper.Writer.utf8_opt
              [| None; Some (sprintf "hello %d" i); Some "world" |]
              ~name:"foobar"
          ; Wrapper.Writer.int [| i; i + 1; 2 * i |] ~name:"baz"
          ; Wrapper.Writer.int_opt [| Some (i / 2); None; None |] ~name:"baz_opt"
          ; Wrapper.Writer.float [| f; f +. 0.5; f /. 2. |] ~name:"fbaz"
          ; Wrapper.Writer.float_opt
              [| None; None; Some (Float.sqrt f) |]
              ~name:"fbaz_opt"
          ; Wrapper.Writer.bitset (Valid.create_all_valid 3) ~name:"bset"
          ]
        in
        Wrapper.Writer.create_table ~cols)
    |> Wrapper.Table.concatenate
  in
  print_s ~mach:() ([%sexp_of: Column.t] (Column.experimental_fast_read t 0));
  print_s ~mach:() ([%sexp_of: Column.t] (Column.experimental_fast_read t 1));
  print_s ~mach:() ([%sexp_of: Column.t] (Column.experimental_fast_read t 2));
  print_s ~mach:() ([%sexp_of: Column.t] (Column.experimental_fast_read t 3));
  print_s ~mach:() ([%sexp_of: Column.t] (Column.experimental_fast_read t 4));
  print_s ~mach:() ([%sexp_of: Column.t] (Column.experimental_fast_read t 5));
  print_s ~mach:() ([%sexp_of: Column.t] (Column.experimental_fast_read t 6));
  [%expect
    {|
    (String(v1 v2 v3 v1 v2 v3 v1 v2 v3 v1 v2 v3 v1 v2 v3 v1 v2 v3 v1 v2 v3 v1 v2 v3 v1 v2 v3 v1 v2 v3 v1 v2 v3 v1 v2 v3))
    (String_option(()("hello 0")(world)()("hello 1")(world)()("hello 2")(world)()("hello 3")(world)()("hello 4")(world)()("hello 5")(world)()("hello 6")(world)()("hello 7")(world)()("hello 8")(world)()("hello 9")(world)()("hello 10")(world)()("hello 11")(world)))
    (Int64(0 1 0 1 2 2 2 3 4 3 4 6 4 5 8 5 6 10 6 7 12 7 8 14 8 9 16 9 10 18 10 11 20 11 12 22))
    (Int64_option((0)()()(0)()()(1)()()(1)()()(2)()()(2)()()(3)()()(3)()()(4)()()(4)()()(5)()()(5)()()))
    (Double(0 0.5 0 1 1.5 0.5 2 2.5 1 3 3.5 1.5 4 4.5 2 5 5.5 2.5 6 6.5 3 7 7.5 3.5 8 8.5 4 9 9.5 4.5 10 10.5 5 11 11.5 5.5))
    (Double_option(()()(0)()()(1)()()(1.4142135623730951)()()(1.7320508075688772)()()(2)()()(2.23606797749979)()()(2.4494897427831779)()()(2.6457513110645907)()()(2.8284271247461903)()()(3)()()(3.1622776601683795)()()(3.3166247903554)))
    Unsupported_type |}];
  let t = Table.slice t ~offset:10 ~length:9 in
  print_s ~mach:() ([%sexp_of: Column.t] (Column.experimental_fast_read t 0));
  print_s ~mach:() ([%sexp_of: Column.t] (Column.experimental_fast_read t 1));
  print_s ~mach:() ([%sexp_of: Column.t] (Column.experimental_fast_read t 2));
  print_s ~mach:() ([%sexp_of: Column.t] (Column.experimental_fast_read t 3));
  print_s ~mach:() ([%sexp_of: Column.t] (Column.experimental_fast_read t 4));
  print_s ~mach:() ([%sexp_of: Column.t] (Column.experimental_fast_read t 5));
  [%expect
    {|
    (String(v2 v3 v1 v2 v3 v1 v2 v3 v1))
    (String_option(("hello 3")(world)()("hello 4")(world)()("hello 5")(world)()))
    (Int64(4 6 4 5 8 5 6 10 6))
    (Int64_option(()()(2)()()(2)()()(3)))
    (Double(3.5 1.5 4 4.5 2 5 5.5 2.5 6))
    (Double_option(()(1.7320508075688772)()()(2)()()(2.23606797749979)())) |}]
