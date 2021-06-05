open Core_kernel
open Arrow_c_api

type t =
  { x : int
  ; y : float
  ; z : string
  ; z_opt : float option
  }
[@@deriving sexp_of, fields, compare]

let `read read, `write write =
  let open F in
  read_write_fn (Fields.make_creator ~x:i64 ~y:f64 ~z:str ~z_opt:f64_opt)

let gen =
  let float_gen = Float.gen_uniform_excl (-1e6) 1e6 in
  let open Quickcheck.Let_syntax in
  let%map x = Int.gen_incl (-1000000) 1000000
  and y = float_gen
  and z = Int.quickcheck_generator
  and z_opt = Option.quickcheck_generator float_gen in
  { x; y; z = Printf.sprintf "foo%d" z; z_opt }

let python_read_and_rewrite ~filename ~print_details =
  let in_channel, out_channel = Caml_unix.open_process "python" in
  let print_details =
    if print_details
    then
      [ "print(df.shape)"
      ; "print(df['z'].iloc[0])"
      ; "print(sum(df['x']), sum(df['y']), np.nansum(df['z_opt']))"
      ]
    else []
  in
  Out_channel.output_lines
    out_channel
    ([ "import os"
     ; "import pandas as pd"
     ; "import numpy as np"
     ; Printf.sprintf "df = pd.read_parquet('%s')" filename
     ]
    @ print_details
    @ [ Printf.sprintf "os.remove('%s')" filename
      ; Printf.sprintf "df.to_parquet('%s')" filename
      ]);
  Out_channel.close out_channel;
  let lines = In_channel.input_lines in_channel in
  In_channel.close in_channel;
  lines

let%expect_test _ =
  let gen =
    let%bind.Quickcheck length = Int.gen_incl 1 10_000 in
    List.gen_with_length length gen
  in
  Quickcheck.iter ~trials:10 ~seed:(`Deterministic "fortytwo") gen ~f:(fun ts ->
      let filename = Caml.Filename.temp_file "test" ".parquet" in
      Exn.protect
        ~f:(fun () ->
          let hd = List.hd_exn ts in
          Stdio.printf "z: %s\n" hd.z;
          let sum_x = List.fold ts ~init:0 ~f:(fun acc t -> acc + t.x) in
          Stdio.printf "sum_x: %d\n" sum_x;
          let sum_y = List.fold ts ~init:0. ~f:(fun acc t -> acc +. t.y) in
          Stdio.printf "sum_y: %f\n" sum_y;
          let sum_z_opt =
            List.fold ts ~init:0. ~f:(fun acc t ->
                acc +. Option.value t.z_opt ~default:0.)
          in
          Stdio.printf "sum_z_opt: %f\n" sum_z_opt;
          write ~chunk_size:128 filename ts;
          let lines = python_read_and_rewrite ~filename ~print_details:true in
          List.iter lines ~f:(Stdio.printf ">> %s\n%!");
          let ts' = read filename in
          let no_diff = ref true in
          List.iter2_exn ts ts' ~f:(fun t t' ->
              if compare t t' <> 0 && !no_diff
              then (
                no_diff := false;
                Stdio.printf
                  "in:  %s\nout: %s\n\n%!"
                  (sexp_of_t t |> Sexp.to_string_mach)
                  (sexp_of_t t' |> Sexp.to_string_mach)));
          Stdio.printf "\n")
        ~finally:(fun () -> Caml.Sys.remove filename));
  [%expect
    {|
    z: foo-55932
    sum_x: -9634663
    sum_y: -19118594.436744
    sum_z_opt: -4445953.971299
    >> (900, 4)
    >> foo-55932
    >> -9634663 -19118594.436743572 -4445953.971298594

    z: foo-28130
    sum_x: -62348033
    sum_y: -5284735.806094
    sum_z_opt: 11311189.703559
    >> (4541, 4)
    >> foo-28130
    >> -62348033 -5284735.806094399 11311189.703559455

    z: foo-14074670860506
    sum_x: -2194031
    sum_y: -49385183.103624
    sum_z_opt: 46092060.535320
    >> (6919, 4)
    >> foo-14074670860506
    >> -2194031 -49385183.10362447 46092060.53532025

    z: foo69514787795017277
    sum_x: 31951719
    sum_y: 9443864.574338
    sum_z_opt: 28098997.268899
    >> (1248, 4)
    >> foo69514787795017277
    >> 31951719 9443864.574338121 28098997.26889878

    z: foo-10481
    sum_x: 109738668
    sum_y: 41833442.778917
    sum_z_opt: -6977142.876697
    >> (7399, 4)
    >> foo-10481
    >> 109738668 41833442.77891723 -6977142.876696713

    z: foo-47345
    sum_x: -23060553
    sum_y: 3703889.308918
    sum_z_opt: 39339689.458739
    >> (9706, 4)
    >> foo-47345
    >> -23060553 3703889.308917751 39339689.45873935

    z: foo23
    sum_x: -18203287
    sum_y: 48466840.772192
    sum_z_opt: 18986853.850194
    >> (2614, 4)
    >> foo23
    >> -18203287 48466840.77219244 18986853.850193717

    z: foo461833321065013564
    sum_x: -14960433
    sum_y: -41488906.679583
    sum_z_opt: -6266360.325795
    >> (2239, 4)
    >> foo461833321065013564
    >> -14960433 -41488906.67958288 -6266360.325795282

    z: foo-1455838189380713883
    sum_x: 62872605
    sum_y: -26803037.585976
    sum_z_opt: 915161.556732
    >> (6619, 4)
    >> foo-1455838189380713883
    >> 62872605 -26803037.585975993 915161.556731455

    z: foo405645133115846
    sum_x: 56118816
    sum_y: 6280585.336361
    sum_z_opt: 12082220.700733
    >> (3286, 4)
    >> foo405645133115846
    >> 56118816 6280585.336360538 12082220.700732507 |}]

let%expect_test _ =
  let filename = Caml.Filename.temp_file "test" ".parquet" in
  Exn.protect
    ~f:(fun () ->
      let col_v1 = Writer.float [| 1.; 2.; 3.; 3.14159265358979; 5. |] ~name:"x" in
      let col_v2 =
        Writer.float_opt
          [| Some 2.718281828; None; None; Some 13.37; None |]
          ~name:"col_v2"
      in
      let col_date =
        let d = Date.of_string "2020-01-01" in
        Writer.date
          [| d; d; Date.add_days d 12; Date.add_days d (-42); d |]
          ~name:"col_date"
      in
      let col_time =
        let t = Time_ns.of_string "2021-06-05 09:36:00.123+01:00" in
        Writer.time_ns [| t; t; t; t; t |] ~name:"col_time"
      in
      Writer.write filename ~cols:[ col_v1; col_v2; col_date; col_time ];
      let lines = python_read_and_rewrite ~filename ~print_details:false in
      List.iter lines ~f:(Stdio.printf ">> %s\n%!");
      let table = Parquet_reader.table filename in
      let rows = Wrapper.Table.num_rows table in
      Stdio.printf "%d\n%!" rows)
    ~finally:(fun () -> Caml.Sys.remove filename);
  [%expect
    {|
    5

|}]
