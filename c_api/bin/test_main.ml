open Base
open Arrow_c_api

module R = struct
  type t =
    { x : int
    ; y : float
    }
  [@@deriving sexp, fields]

  let read = F.Reader.(read (Fields.make_creator ~x:i64 ~y:f64))

  let () =
    let filename =
      match Caml.Sys.argv with
      | [| _exe; filename |] -> filename
      | _ -> Printf.failwithf "usage: %s file.parquet" Caml.Sys.argv.(0) ()
    in
    Reader.with_file filename ~f:(fun reader ->
        Schema.get reader
        |> Schema.sexp_of_t
        |> Sexp.to_string_hum
        |> Stdio.printf "Read schema:\n%s\n%!");
    let ts = read filename in
    List.iteri ts ~f:(fun i t ->
        Stdio.printf "%d %s\n%!" i (sexp_of_t t |> Sexp.to_string_mach))
end

module W = struct
  type t =
    { x : int
    ; y : float
    ; z : string
    }
  [@@deriving sexp, fields]

  let write = F.Writer.(write (Fields.fold ~x:i64 ~y:f64 ~z:str))

  let () =
    let ts =
      [ { x = 42; y = 3.14159265358979; z = "foo" }
      ; { x = 1337; y = 2.71828182846; z = "bar" }
      ; { x = 299792458; y = 6.02214e23; z = "foobar" }
      ]
    in
    write "/tmp/abc.parquet" ts
end
