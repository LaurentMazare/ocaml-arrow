open Core_kernel
open Arrow_c_api

let%expect_test _ =
  let t =
    List.init 12 ~f:(fun i ->
        let cols =
          [ Wrapper.Writer.utf8 [| "v1"; "v2"; "v3" |] ~name:"foo"
          ; Wrapper.Writer.utf8_opt
              [| None; Some (sprintf "hello %d" i); Some "world" |]
              ~name:"foobar"
          ]
        in
        Wrapper.Writer.create_table ~cols)
    |> Wrapper.Table.concatenate
  in
  print_s ~mach:() ([%sexp_of: Column.t] (Column.experimental_fast_read t 0));
  print_s ~mach:() ([%sexp_of: Column.t] (Column.experimental_fast_read t 1));
  [%expect
    {|
    (String(v1 v2 v3 v1 v2 v3 v1 v2 v3 v1 v2 v3 v1 v2 v3 v1 v2 v3 v1 v2 v3 v1 v2 v3 v1 v2 v3 v1 v2 v3 v1 v2 v3 v1 v2 v3))
    (String_option(()("hello 0")(world)()("hello 1")(world)()("hello 2")(world)()("hello 3")(world)()("hello 4")(world)()("hello 5")(world)()("hello 6")(world)()("hello 7")(world)()("hello 8")(world)()("hello 9")(world)()("hello 10")(world)()("hello 11")(world))) |}];
  let t = Table.slice t ~offset:8 ~length:5 in
  print_s ~mach:() ([%sexp_of: Column.t] (Column.experimental_fast_read t 0));
  print_s ~mach:() ([%sexp_of: Column.t] (Column.experimental_fast_read t 1));
  [%expect
    {|
    (String(v3 v1 v2 v3 v1))
    (String_option((world)()("hello 3")(world)())) |}]
