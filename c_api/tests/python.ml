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

let python_read_and_rewrite ~filename =
  let in_channel, out_channel = Unix.open_process "python" in
  Out_channel.output_lines
    out_channel
    [ "import os"
    ; "import pandas as pd"
    ; "import numpy as np"
    ; Printf.sprintf "df = pd.read_parquet('%s')" filename
    ; "print(df.shape)"
    ; "print(df['z'].iloc[0])"
    ; "print(sum(df['x']), sum(df['y']), np.nansum(df['z_opt']))"
    ; Printf.sprintf "os.remove('%s')" filename
    ; Printf.sprintf "df.to_parquet('%s')" filename
    ];
  Out_channel.close out_channel;
  let lines = In_channel.input_lines in_channel in
  In_channel.close in_channel;
  lines

let%expect_test _ =
  let gen = List.gen_with_length 1000 gen in
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
          let lines = python_read_and_rewrite ~filename in
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
    z: foo31357086028138773
    sum_x: 1615492
    sum_y: -18349446.767035
    sum_z_opt: -8595834.583423
    >> (1000, 4)
    >> foo31357086028138773
    >> 1615492 -18349446.767035134 -8595834.583422668

    z: foo37406735863699853
    sum_x: -2218751
    sum_y: 33700159.632548
    sum_z_opt: 9143076.970966
    >> (1000, 4)
    >> foo37406735863699853
    >> -2218751 33700159.63254831 9143076.970966265

    z: foo34262
    sum_x: -10344926
    sum_y: 440499.384364
    sum_z_opt: 4563392.080529
    >> (1000, 4)
    >> foo34262
    >> -10344926 440499.38436365844 4563392.080529071

    z: foo17
    sum_x: 12960647
    sum_y: -6305263.362449
    sum_z_opt: -17370605.547353
    >> (1000, 4)
    >> foo17
    >> 12960647 -6305263.3624486765 -17370605.54735332

    z: foo142076699827
    sum_x: -17800561
    sum_y: -16546258.700482
    sum_z_opt: 13679441.389470
    >> (1000, 4)
    >> foo142076699827
    >> -17800561 -16546258.700481601 13679441.389470028

    z: foo-7230679753750831
    sum_x: 1674633
    sum_y: -34150208.526523
    sum_z_opt: 119607.860948
    >> (1000, 4)
    >> foo-7230679753750831
    >> 1674633 -34150208.52652328 119607.86094824644

    z: foo-607835342471572
    sum_x: -17037115
    sum_y: 6393302.508115
    sum_z_opt: 6608752.877545
    >> (1000, 4)
    >> foo-607835342471572
    >> -17037115 6393302.508115031 6608752.87754469

    z: foo-117784
    sum_x: 22980538
    sum_y: 6048539.142303
    sum_z_opt: -6059385.581084
    >> (1000, 4)
    >> foo-117784
    >> 22980538 6048539.142303257 -6059385.581084397

    z: foo23046
    sum_x: -19939884
    sum_y: -51992627.094515
    sum_z_opt: 2561442.267542
    >> (1000, 4)
    >> foo23046
    >> -19939884 -51992627.094514504 2561442.2675415524

    z: foo312251
    sum_x: 26022909
    sum_y: -4440303.778826
    sum_z_opt: 27285365.302144
    >> (1000, 4)
    >> foo312251
    >> 26022909 -4440303.778826275 27285365.302144397 |}]
