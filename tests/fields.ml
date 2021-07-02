open Base
open Arrow_c_api

type time = Core_kernel.Time_ns.t [@@deriving compare]

let sexp_of_time time = Core_kernel.Time_ns.to_string time |> sexp_of_string

type t =
  { x : int
  ; y : float
  ; z : string
  ; truc : Core_kernel.Date.t
  ; time : time
  ; y_opt : float option
  ; z_opt : string option
  ; cnt : int option
  }
[@@deriving sexp_of, fields, compare]

let `read read, `write write =
  let open F in
  read_write_fn
    (Fields.make_creator
       ~x:i64
       ~y:f64
       ~z:str
       ~truc:date
       ~time:time_ns
       ~y_opt:f64_opt
       ~z_opt:str_opt
       ~cnt:i64_opt)

let generate_ts ~cnt =
  let base_time = Core_kernel.Time_ns.now () in
  let base_date = Core_kernel.Date.of_string "2020-01-16" in
  let date = Core_kernel.Date.add_days base_date in
  let time s = Core_kernel.Time_ns.(add base_time (Span.of_sec s)) in
  let ts ~cnt =
    [ { x = 42
      ; y = 3.14159265358979
      ; z = "foo"
      ; truc = date 1
      ; time = time 0.
      ; y_opt = Some 1.414
      ; z_opt = None
      ; cnt = Some cnt
      }
    ; { x = 42
      ; y = Random.float 1.
      ; z = "foo"
      ; truc = date 1
      ; time = time 0.
      ; y_opt = None
      ; z_opt = Some (Int.to_string cnt)
      ; cnt = Some cnt
      }
    ; { x = 1337
      ; y = 2.71828182846
      ; z = "bar"
      ; truc = date 0
      ; time = time 123.45
      ; y_opt = None
      ; z_opt = Some "here!"
      ; cnt = Some cnt
      }
    ; { x = 299792458
      ; y = 6.02214e23
      ; z = "foobar"
      ; truc = date 5
      ; time = time 987654.
      ; y_opt = Some 1.732
      ; z_opt = Some "here again!"
      ; cnt = None
      }
    ]
  in
  List.init cnt ~f:(fun cnt -> ts ~cnt) |> List.concat

let run ?chunk_size ?compression cnt =
  let filename = Caml.Filename.temp_file "test" ".parquet" in
  Exn.protect
    ~f:(fun () ->
      let ts = generate_ts ~cnt in
      write ?chunk_size ?compression filename ts;
      let ts' = read filename in
      let no_diff = ref true in
      List.iter2_exn ts ts' ~f:(fun t t' ->
          if compare t t' <> 0 && !no_diff
          then (
            no_diff := false;
            Stdio.printf
              "in:  %s\nout: %s\n\n%!"
              (sexp_of_t t |> Sexp.to_string_mach)
              (sexp_of_t t' |> Sexp.to_string_mach))))
    ~finally:(fun () -> Caml.Sys.remove filename)

let%expect_test _ =
  run 0;
  run 1;
  run 10;
  run 100;
  run 1000;
  run ~chunk_size:128 1000;
  run ~compression:Lz4 100000;
  run ~chunk_size:1024 100000;
  [%expect {| |}]
