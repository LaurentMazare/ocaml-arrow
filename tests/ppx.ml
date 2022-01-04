open Base

module Test1 = struct
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
    Array.iter ts ~f:(fun t ->
        sexp_of_t t |> Sexp.to_string_mach |> Stdio.printf "%s\n%!");
    [%expect
      {|
    ((x 42)(y 3.14159265358979)(z foobar))
    ((x 1337)(y 2.71828182846)(z foo))
    ((x 1295)(y 0.123456)(z bar)) |}]
end

module Foobar = struct
  type t =
    | Foo
    | Bar
    | Foobar
  [@@deriving sexp_of]

  let to_string = function
    | Foo -> "Foo"
    | Bar -> "Bar"
    | Foobar -> "FooBar"

  let of_string = function
    | "Foo" -> Foo
    | "Bar" -> Bar
    | "FooBar" -> Foobar
    | otherwise -> Printf.failwithf "unknown variant %s" otherwise ()

  let to_int_exn = function
    | Foo -> 1
    | Bar -> 2
    | Foobar -> 3

  let of_int_exn = function
    | 1 -> Foo
    | 2 -> Bar
    | 3 -> Foobar
    | otherwise -> Printf.failwithf "unknown variant %d" otherwise ()
end

module Test2 = struct
  type t =
    { x : int
    ; y : float
    ; z : Foobar.t [@arrow.stringable]
    }
  [@@deriving arrow, sexp_of]

  let%expect_test _ =
    let ts =
      [| { x = 42; y = 3.14159265358979; z = Foobar }
       ; { x = 1337; y = 2.71828182846; z = Foo }
       ; { x = 1337 - 42; y = 0.123456; z = Bar }
      |]
    in
    let filename = "/tmp/abc.parquet" in
    arrow_write_t ts filename;
    let ts = arrow_read_t filename in
    Array.iter ts ~f:(fun t ->
        sexp_of_t t |> Sexp.to_string_mach |> Stdio.printf "%s\n%!");
    [%expect
      {|
    ((x 42)(y 3.14159265358979)(z Foobar))
    ((x 1337)(y 2.71828182846)(z Foo))
    ((x 1295)(y 0.123456)(z Bar)) |}];
    let ts = Test1.arrow_read_t filename in
    Array.iter ts ~f:(fun t ->
        Test1.sexp_of_t t |> Sexp.to_string_mach |> Stdio.printf "%s\n%!");
    [%expect
      {|
      ((x 42)(y 3.14159265358979)(z FooBar))
      ((x 1337)(y 2.71828182846)(z Foo))
      ((x 1295)(y 0.123456)(z Bar)) |}]
end

module Test3 = struct
  type t =
    { x : Foobar.t [@arrow.intable]
    ; y : float
    ; z : Foobar.t [@arrow.stringable]
    }
  [@@deriving arrow, sexp_of]

  let%expect_test _ =
    let ts =
      [| { x = Foo; y = 3.14159265358979; z = Foobar }
       ; { x = Foo; y = 2.71828182846; z = Foo }
       ; { x = Bar; y = 0.123456; z = Bar }
      |]
    in
    let filename = "/tmp/abc.parquet" in
    arrow_write_t ts filename;
    let ts = arrow_read_t filename in
    Array.iter ts ~f:(fun t ->
        sexp_of_t t |> Sexp.to_string_mach |> Stdio.printf "%s\n%!");
    [%expect
      {|
    ((x Foo)(y 3.14159265358979)(z Foobar))
    ((x Foo)(y 2.71828182846)(z Foo))
    ((x Bar)(y 0.123456)(z Bar)) |}];
    let ts = Test1.arrow_read_t filename in
    Array.iter ts ~f:(fun t ->
        Test1.sexp_of_t t |> Sexp.to_string_mach |> Stdio.printf "%s\n%!");
    [%expect
      {|
      ((x 1)(y 3.14159265358979)(z FooBar))
      ((x 1)(y 2.71828182846)(z Foo))
      ((x 2)(y 0.123456)(z Bar)) |}]
end

module Test4 = struct
  module Str = struct
    type t = string [@@deriving sexp_of]

    let of_int_exn = Int.to_string
    let to_int_exn = Int.of_string
  end

  type t =
    { x : Str.t [@arrow.intable]
    ; y : float
    ; z : Foobar.t option [@arrow.stringable]
    ; fl : Int.t option [@arrow.floatable]
    }
  [@@deriving arrow, sexp_of]

  module Raw = struct
    type t =
      { x : int
      ; y : float
      ; z : string option
      ; fl : float option
      }
    [@@deriving arrow, sexp_of]
  end

  let%expect_test _ =
    let ts =
      [| { x = "1234"; y = 3.14159265358979; z = Some Foobar; fl = None }
       ; { x = "5678"; y = 2.71828182846; z = Some Foo; fl = Some 42 }
       ; { x = "-123"; y = 0.123456; z = None; fl = Some (-1337) }
      |]
    in
    let filename = "/tmp/abc.parquet" in
    arrow_write_t ts filename;
    let ts = arrow_read_t filename in
    Array.iter ts ~f:(fun t ->
        sexp_of_t t |> Sexp.to_string_mach |> Stdio.printf "%s\n%!");
    [%expect
      {|
    ((x 1234)(y 3.14159265358979)(z(Foobar))(fl()))
    ((x 5678)(y 2.71828182846)(z(Foo))(fl(42)))
    ((x -123)(y 0.123456)(z())(fl(-1337))) |}];
    let raws = Raw.arrow_read_t filename in
    Array.iter raws ~f:(fun raw ->
        Raw.sexp_of_t raw |> Sexp.to_string_mach |> Stdio.printf "%s\n%!");
    [%expect
      {|
    ((x 1234)(y 3.14159265358979)(z(FooBar))(fl()))
    ((x 5678)(y 2.71828182846)(z(Foo))(fl(42)))
    ((x -123)(y 0.123456)(z())(fl(-1337))) |}]
end

module Test5 = struct
  open Core_kernel

  module Foo = struct
    type t =
      { left : string
      ; right : int
      }
    [@@deriving sexp]
  end

  type t =
    { x : int
    ; y : float [@arrow.sexpable]
    ; z : string
    ; foo : Foo.t [@arrow.sexpable]
    ; bar : int * float [@arrow.sexpable]
    }
  [@@deriving arrow, sexp_of]

  let%expect_test _ =
    let ts =
      [| { x = 42
         ; y = 3.14159265358979
         ; z = "foobar"
         ; foo = { left = "1 2"; right = 5 }
         ; bar = 1, 2.
         }
       ; { x = 1337
         ; y = 2.71828182846
         ; z = "foo"
         ; foo = { left = "l"; right = 14 }
         ; bar = 3, 4.
         }
       ; { x = 1337 - 42
         ; y = 0.123456
         ; z = "bar"
         ; foo = { left = "ll"; right = 42 }
         ; bar = 5, 6.78
         }
      |]
    in
    let filename = "/tmp/abc.parquet" in
    arrow_write_t ts filename;
    let ts = arrow_read_t filename in
    Array.iter ts ~f:(fun t ->
        sexp_of_t t |> Sexp.to_string_mach |> Stdio.printf "%s\n%!");
    [%expect
      {|
    ((x 42)(y 3.14159265358979)(z foobar)(foo((left"1 2")(right 5)))(bar(1 2)))
    ((x 1337)(y 2.71828182846)(z foo)(foo((left l)(right 14)))(bar(3 4)))
    ((x 1295)(y 0.123456)(z bar)(foo((left ll)(right 42)))(bar(5 6.78))) |}]
end

module Test6 = struct
  type t =
    { p : int
    ; is_prime : bool
    ; is_prime_opt : bool option
    ; largest_prime : int option
    }
  [@@deriving arrow, sexp_of]

  let%expect_test _ =
    let ts =
      let max_n = 40 in
      let prime_div = Array.init (max_n + 1) ~f:Fn.id in
      for p = 2 to max_n do
        if prime_div.(p) = p
        then (
          let q = ref p in
          while !q <= max_n do
            prime_div.(!q) <- p;
            q := !q + p
          done)
      done;
      Array.init 20 ~f:(fun p ->
          let p = p + 1 in
          let is_prime = prime_div.(p) = p in
          let is_prime_opt = if p % 2 <> 0 then Some is_prime else None in
          let largest_prime = if is_prime then None else Some prime_div.(p) in
          { p; is_prime; is_prime_opt; largest_prime })
    in
    let filename = "/tmp/abc.parquet" in
    arrow_write_t ts filename;
    let ts = arrow_read_t filename in
    Array.iter ts ~f:(fun t ->
        sexp_of_t t |> Sexp.to_string_mach |> Stdio.printf "%s\n%!");
    [%expect
      {|
    ((x 42)(y 3.14159265358979)(z foobar))
    ((x 1337)(y 2.71828182846)(z foo))
    ((x 1295)(y 0.123456)(z bar)) |}]
end
