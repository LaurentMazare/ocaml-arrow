open Base
open Ppx_arrow_runtime

type t =
  { x : int
  ; y : float
  ; z : string
  }
[@@deriving arrow, sexp_of]

let%expect_test _ =
  let ts =
    [| { x = 42; y = 3.14159265358979; z = "foobar" }
     ; { x = 1337; y = 2.71828182846; z = "foo" }
     ; { x = 1337 - 42; y = 0.123456; z = "bar" }
    |]
  in
  let filename = "/tmp/abc.parquet" in
  arrow_write_t ts filename;
  let ts = arrow_read_t filename in
  Array.iter ts ~f:(fun t -> sexp_of_t t |> Sexp.to_string_mach |> Stdio.printf "%s\n%!");
  [%expect
    {|
    ((x 42)(y 3.14159265358979)(z foobar))
    ((x 1337)(y 2.71828182846)(z foo))
    ((x 1295)(y 0.123456)(z bar)) |}]
