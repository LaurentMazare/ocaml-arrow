open Core_kernel
open Arrow_c_api

let%expect_test _ =
  (try ignore (Parquet_reader.table "does-not-exist.parquet" : Table.t) with
  | exn -> Stdio.printf "%s\n%!" (Exn.to_string exn));
  [%expect
    {|
    (Failure
      "IOError: Failed to open local file 'does-not-exist.parquet'. Detail: [errno 2] No such file or directory") |}]

let%expect_test _ =
  let filename = Caml.Filename.temp_file "test" ".parquet" in
  Exn.protect
    ~f:(fun () ->
      try ignore (Parquet_reader.table filename : Table.t) with
      | exn -> Stdio.printf "%s\n%!" (Exn.to_string exn))
    ~finally:(fun () -> Caml.Sys.remove filename);
  [%expect {|
    (Failure "Invalid: Parquet file size is 0 bytes") |}]
