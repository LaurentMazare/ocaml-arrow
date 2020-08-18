open Core_kernel
open Arrow_c_api

let bitset_to_string bitset =
  List.init (Valid.length bitset) ~f:(fun i -> if Valid.get bitset i then '1' else '0')
  |> String.of_char_list

let bitset_opt_to_string bitset ~valid =
  List.init (Valid.length bitset) ~f:(fun i ->
      if Valid.get valid i then if Valid.get bitset i then '1' else '0' else ' ')
  |> String.of_char_list

let python_read_and_rewrite ~filename =
  let in_channel, out_channel = Unix.open_process "python" in
  Out_channel.output_lines
    out_channel
    [ "import os"
    ; "import pandas as pd"
    ; "import numpy as np"
    ; Printf.sprintf "df = pd.read_parquet('%s')" filename
    ; "for col in df.columns:"
    ; "  print(''.join([' ' if b is None else '1' if b else '0' for b in list(df[col])]))"
    ; Printf.sprintf "os.remove('%s')" filename
    ; Printf.sprintf "df.to_parquet('%s')" filename
    ];
  Out_channel.close out_channel;
  let lines = In_channel.input_lines in_channel in
  In_channel.close in_channel;
  lines

let%expect_test _ =
  let test len ~chunk_size =
    let filename = Caml.Filename.temp_file "test" ".parquet" in
    Exn.protect
      ~f:(fun () ->
        let bitsets =
          List.init 32 ~f:(fun _ ->
              let bitset = Valid.create_all_valid len in
              for i = 0 to len - 1 do
                Valid.set bitset i (Random.bool ())
              done;
              bitset)
        in
        let cols =
          List.mapi bitsets ~f:(fun i bitset ->
              Wrapper.Writer.bitset bitset ~name:(Int.to_string i))
        in
        Wrapper.Writer.write ~chunk_size filename ~cols;
        let py_bitsets = python_read_and_rewrite ~filename in
        let table = Parquet_reader.table filename ~column_idxs:[] in
        assert (List.length py_bitsets = 32);
        let py_bitsets = Array.of_list py_bitsets in
        List.iteri bitsets ~f:(fun i bitset ->
            let py_bitset = py_bitsets.(i) in
            let bitset = bitset_to_string bitset in
            let bitset' =
              Wrapper.Column.read_bitset table ~column:(`Name (Int.to_string i))
              |> bitset_to_string
            in
            if String.( <> ) bitset bitset' then Stdio.printf "%s\n%s\n\n" bitset bitset';
            if String.( <> ) bitset py_bitset
            then Stdio.printf "ml: %s\npy: %s\n\n" bitset py_bitset))
      ~finally:(fun () -> Caml.Sys.remove filename)
  in
  List.iter
    ~f:(fun (len, chunk_size) -> test len ~chunk_size)
    [ 16, 32; 32, 32; 32, 31; 32, 11; 32, 16; 69, 32; 69, 27 ];
  [%expect {||}]

let%expect_test _ =
  let test len ~chunk_size =
    let filename = Caml.Filename.temp_file "test" ".parquet" in
    Exn.protect
      ~f:(fun () ->
        let bitset_and_valids =
          List.init 32 ~f:(fun _ ->
              let rnd () =
                let bitset = Valid.create_all_valid len in
                for i = 0 to len - 1 do
                  Valid.set bitset i (Random.bool ())
                done;
                bitset
              in
              rnd (), rnd ())
        in
        let cols =
          List.mapi bitset_and_valids ~f:(fun i (bitset, valid) ->
              Wrapper.Writer.bitset_opt bitset ~valid ~name:(Int.to_string i))
        in
        Wrapper.Writer.write ~chunk_size filename ~cols;
        let py_bitsets = python_read_and_rewrite ~filename in
        let table = Parquet_reader.table filename ~column_idxs:[] in
        assert (List.length py_bitsets = 32);
        let py_bitsets = Array.of_list py_bitsets in
        List.iteri bitset_and_valids ~f:(fun i (bitset, valid) ->
            let py_bitset = py_bitsets.(i) in
            let bitset = bitset_opt_to_string bitset ~valid in
            let bitset', valid' =
              Wrapper.Column.read_bitset_opt table ~column:(`Name (Int.to_string i))
            in
            let bitset' = bitset_opt_to_string bitset' ~valid:valid' in
            if String.( <> ) bitset bitset' then Stdio.printf "%s\n%s\n\n" bitset bitset';
            if String.( <> ) bitset py_bitset
            then Stdio.printf "ml: %s\npy: %s\n\n" bitset py_bitset))
      ~finally:(fun () -> Caml.Sys.remove filename)
  in
  List.iter
    ~f:(fun (len, chunk_size) -> test len ~chunk_size)
    [ 16, 32; 32, 32; 32, 31; 32, 11; 32, 16; 69, 32; 69, 27 ];
  [%expect {||}]

let%expect_test _ =
  let run len =
    let bitset_and_valids =
      List.init 32 ~f:(fun _ ->
          let rnd () =
            let bitset = Valid.create_all_valid len in
            for i = 0 to len - 1 do
              Valid.set bitset i (Random.bool ())
            done;
            bitset
          in
          rnd (), rnd ())
    in
    let cols =
      List.mapi bitset_and_valids ~f:(fun i (bitset, valid) ->
          Wrapper.Writer.bitset_opt bitset ~valid ~name:(Int.to_string i))
    in
    let table =
      Wrapper.Writer.create_table ~cols |> Wrapper.Table.slice ~offset:0 ~length:10000
    in
    List.iteri bitset_and_valids ~f:(fun i (bitset, valid) ->
        let bitset = bitset_opt_to_string bitset ~valid in
        let bitset', valid' =
          Wrapper.Column.read_bitset_opt table ~column:(`Name (Int.to_string i))
        in
        let bitset' = bitset_opt_to_string bitset' ~valid:valid' in
        if String.( <> ) bitset bitset' then Stdio.printf "%s\n%s\n\n" bitset bitset')
  in
  List.iter [ 16; 23; 61 ] ~f:run
