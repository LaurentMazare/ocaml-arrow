open Core_kernel
open Arrow_c_api

let catch_and_print f =
  try f () with
  | exn -> Stdio.printf "%s\n%!" (Exn.to_string exn)

let%expect_test _ =
  catch_and_print (fun () ->
      ignore (Parquet_reader.table "does-not-exist.parquet" : Table.t));
  [%expect
    {|
    (Failure
      "IOError: Failed to open local file 'does-not-exist.parquet'. Detail: [errno 2] No such file or directory") |}]

let%expect_test _ =
  let filename = Caml.Filename.temp_file "test" ".parquet" in
  Exn.protect
    ~f:(fun () ->
      catch_and_print (fun () -> ignore (Parquet_reader.table filename : Table.t)))
    ~finally:(fun () -> Caml.Sys.remove filename);
  [%expect {|
    (Failure "Invalid: Parquet file size is 0 bytes") |}]

let%expect_test _ =
  let table =
    List.init 3 ~f:(fun i ->
        let cols =
          [ Wrapper.Writer.utf8 [| "v1"; "v2"; "v3" |] ~name:"foo"
          ; Wrapper.Writer.int [| i; 5 * i; 10 * i |] ~name:"bar"
          ; Wrapper.Writer.int_opt [| Some ((i * 2) + 1); None; None |] ~name:"baz"
          ]
        in
        Wrapper.Writer.create_table ~cols)
    |> Wrapper.Table.concatenate
  in
  catch_and_print (fun () ->
      let _col = Wrapper.Column.read_utf8 table ~column:(`Name "foo") in
      let _col = Wrapper.Column.read_utf8 table ~column:(`Name "foobar") in
      ());
  [%expect {|
    (Failure "cannot find column foobar") |}];
  catch_and_print (fun () ->
      let _col = Wrapper.Column.read_utf8 table ~column:(`Name "baz") in
      ());
  [%expect {|
    (Failure "expected type with utf8 (id 13) got int64") |}];
  catch_and_print (fun () ->
      let _col = Wrapper.Column.read_utf8 table ~column:(`Index 0) in
      let _col = Wrapper.Column.read_utf8 table ~column:(`Index 123) in
      ());
  [%expect {|
    (Failure "invalid column index 123 (ncols: 3)") |}]
