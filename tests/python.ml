open Core_kernel
open Arrow_c_api

type t =
  { x : int
  ; y : float
  ; z : string
  ; z_opt : float option
  ; b : bool
  ; b_opt : bool option
  }
[@@deriving sexp_of, fields, compare]

let `read read, `write write =
  let open F in
  Fields.make_creator ~x:i64 ~y:f64 ~z:str ~z_opt:f64_opt ~b:bool ~b_opt:bool_opt
  |> read_write_fn

let gen =
  let float_gen = Float.gen_uniform_excl (-1e6) 1e6 in
  let open Quickcheck.Let_syntax in
  let%map x = Int.gen_incl (-1000000) 1000000
  and y = float_gen
  and z = Int.quickcheck_generator
  and z_opt = Option.quickcheck_generator float_gen
  and b = Bool.quickcheck_generator
  and b_opt = Option.quickcheck_generator Bool.quickcheck_generator in
  { x; y; z = Printf.sprintf "foo%d" z; z_opt; b; b_opt }

let python_read_and_rewrite ~filename ~print_details =
  let in_channel, out_channel = Caml_unix.open_process "python" in
  let print_details =
    if print_details
    then
      [ "print(df.shape)"
      ; "print(df['z'].iloc[0])"
      ; "print(sum(df['x']), sum(df['y']), np.nansum(df['z_opt']))"
      ; "print(sum(df['b']), sum(df['b_opt'].astype(float).fillna(10**6)))"
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
          let sum_b = List.fold ts ~init:0 ~f:(fun acc t -> acc + if t.b then 1 else 0) in
          Stdio.printf "sum_b: %d\n" sum_b;
          let sum_b1 =
            List.fold ts ~init:0 ~f:(fun acc t ->
                acc
                +
                match t.b_opt with
                | None -> 1
                | Some _ -> 0)
          in
          let sum_b2 =
            List.fold ts ~init:0 ~f:(fun acc t ->
                acc
                +
                match t.b_opt with
                | Some true -> 1
                | Some false | None -> 0)
          in
          Stdio.printf "sum_b_opt: %d %d\n" sum_b1 sum_b2;
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
    sum_x: -29583892
    sum_y: -1606806.135343
    sum_z_opt: -7181519.032317
    sum_b: 447
    sum_b_opt: 475 212
    >> (900, 6)
    >> foo-55932
    >> -29583892 -1606806.1353425747 -7181519.032317471
    >> 447 475000212.0

    z: foo-45
    sum_x: -70833733
    sum_y: 36206088.482584
    sum_z_opt: 7091351.854805
    sum_b: 2708
    sum_b_opt: 2603 1361
    >> (5223, 6)
    >> foo-45
    >> -70833733 36206088.48258363 7091351.854804993
    >> 2708 2603001361.0

    z: foo2598252
    sum_x: -757868
    sum_y: -614689.470489
    sum_z_opt: 361861.158869
    sum_b: 1
    sum_b_opt: 1 0
    >> (1, 6)
    >> foo2598252
    >> -757868 -614689.4704887881 361861.1588694445
    >> 1 1000000.0

    z: foo1076282
    sum_x: -669671
    sum_y: -28063312.331175
    sum_z_opt: 38821493.026763
    sum_b: 3198
    sum_b_opt: 3194 1607
    >> (6446, 6)
    >> foo1076282
    >> -669671 -28063312.33117541 38821493.02676311
    >> 3198 3194001607.0

    z: foo609249368422154
    sum_x: 13370204
    sum_y: 3993934.619034
    sum_z_opt: 1517418.813875
    sum_b: 1298
    sum_b_opt: 1332 659
    >> (2627, 6)
    >> foo609249368422154
    >> 13370204 3993934.6190337846 1517418.8138750556
    >> 1298 1332000659.0

    z: foo48647770842302457
    sum_x: 63283851
    sum_y: 34915730.561748
    sum_z_opt: 6679416.315610
    sum_b: 1054
    sum_b_opt: 1044 549
    >> (2100, 6)
    >> foo48647770842302457
    >> 63283851 34915730.561747685 6679416.3156096125
    >> 1054 1044000549.0

    z: foo46963576856337718
    sum_x: 6385519
    sum_y: -66105313.513491
    sum_z_opt: -27291947.752573
    sum_b: 1743
    sum_b_opt: 1802 842
    >> (3519, 6)
    >> foo46963576856337718
    >> 6385519 -66105313.51349127 -27291947.752572805
    >> 1743 1802000842.0

    z: foo-901387614447954
    sum_x: -32942681
    sum_y: -63259224.299234
    sum_z_opt: 19546809.552908
    sum_b: 1265
    sum_b_opt: 1247 622
    >> (2487, 6)
    >> foo-901387614447954
    >> -32942681 -63259224.2992335 19546809.552908298
    >> 1265 1247000622.0

    z: foo-687404271018784
    sum_x: -34775365
    sum_y: -17426626.705024
    sum_z_opt: -3929344.742169
    sum_b: 623
    sum_b_opt: 596 344
    >> (1258, 6)
    >> foo-687404271018784
    >> -34775365 -17426626.705024164 -3929344.74216865
    >> 623 596000344.0

    z: foo-296
    sum_x: -45829821
    sum_y: -25790516.799683
    sum_z_opt: 12365958.051864
    sum_b: 3398
    sum_b_opt: 3300 1757
    >> (6760, 6)
    >> foo-296
    >> -45829821 -25790516.79968277 12365958.051864266
    >> 3398 3300001757.0 |}]

let sexp_of_time_ns time_ns =
  Time_ns.to_string_iso8601_basic time_ns ~zone:Time.Zone.utc |> sexp_of_string

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
      let rows = Table.num_rows table in
      Stdio.printf "%d\n%!" rows;
      let col_v2 = Column.read_float_opt table ~column:(`Name "col_v2") in
      let col_date = Column.read_date table ~column:(`Name "col_date") in
      let col_time = Column.read_time_ns table ~column:(`Name "col_time") in
      Stdio.printf
        "%s\n%s\n%s\n%!"
        ([%sexp_of: float option array] col_v2 |> Sexp.to_string_mach)
        ([%sexp_of: Date.t array] col_date |> Sexp.to_string_mach)
        ([%sexp_of: time_ns array] col_time |> Sexp.to_string_mach);
      ())
    ~finally:(fun () -> Caml.Sys.remove filename);
  [%expect
    {|
    5
    ((2.718281828)()()(13.37)())
    (2020-01-01 2020-01-01 2020-01-13 2019-11-20 2020-01-01)
    (2021-06-05T08:36:00.123000000Z 2021-06-05T08:36:00.123000000Z 2021-06-05T08:36:00.123000000Z 2021-06-05T08:36:00.123000000Z 2021-06-05T08:36:00.123000000Z)

|}]
