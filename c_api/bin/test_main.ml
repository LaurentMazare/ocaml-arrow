open Base
module F = Arrow_c_api.F
module W = Arrow_c_api.Wrapper

type t =
  { x : int
  ; y : float
  }
[@@deriving sexp, fields]

let read = F.read (Fields.make_creator ~x:F.i64 ~y:F.f64)

let () =
  let filename =
    match Caml.Sys.argv with
    | [| _exe; filename |] -> filename
    | _ -> Printf.failwithf "usage: %s file.parquet" Caml.Sys.argv.(0) ()
  in
  W.Reader.with_file filename ~f:(fun reader ->
      W.Schema.get reader
      |> W.Schema.sexp_of_t
      |> Sexp.to_string_hum
      |> Stdio.printf "Read schema:\n%s\n%!");
  let ts = read filename in
  List.iteri ts ~f:(fun i t ->
      Stdio.printf "%d %s\n%!" i (sexp_of_t t |> Sexp.to_string_mach))
