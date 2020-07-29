open Base
open Arrow_c_api

type time = Core_kernel.Time_ns.t

let sexp_of_time time = Core_kernel.Time_ns.to_string time |> sexp_of_string

module R = struct
  type t =
    { x : int
    ; y : float
    ; truc : Core_kernel.Date.t
    ; time : time
    }
  [@@deriving sexp_of, fields]

  let read = F.Reader.(read (Fields.make_creator ~x:i64 ~y:f64 ~truc:date ~time:time_ns))

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
    ; truc : Core_kernel.Date.t
    ; time : time
    }
  [@@deriving sexp_of, fields]

  let write = F.Writer.(write (Fields.fold ~x:i64 ~y:f64 ~z:str ~truc:date ~time:time_ns))

  let () =
    let base_time = Core_kernel.Time_ns.now () in
    let base_date = Core_kernel.Date.of_string "2020-01-16" in
    let date = Core_kernel.Date.add_days base_date in
    let time s = Core_kernel.Time_ns.(add base_time (Span.of_sec s)) in
    let ts =
      [ { x = 42; y = 3.14159265358979; z = "foo"; truc = date 1; time = time 0. }
      ; { x = 1337; y = 2.71828182846; z = "bar"; truc = date 0; time = time 123.45 }
      ; { x = 299792458
        ; y = 6.02214e23
        ; z = "foobar"
        ; truc = date 5
        ; time = time 987654.
        }
      ]
    in
    write "/tmp/abc.parquet" ts
end
