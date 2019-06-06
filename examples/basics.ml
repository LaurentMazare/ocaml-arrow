open Base
module C = Arrow_core.Wrapper
module A = Arrow.Array

let hello_world () =
  let arr = C.NullArray.new_ (Int64.of_int_exn 42) in
  let len = C.Array.get_length arr in
  Stdio.printf "Hello World! %d\n%!" (Int64.to_int_exn len)

(* Similar to arrow-glib/example/build.c *)
let build () =
  let builder = C.Int32ArrayBuilder.new_ () in
  for i = 0 to 10 do
    let ok = C.Int32ArrayBuilder.append_value builder (Int32.of_int_exn i) in
    assert ok
  done;
  let ok =
    C.Int32ArrayBuilder.append_values
      builder
      (List.map [ 3; 1; 4 ] ~f:Int32.of_int_exn)
      []
  in
  assert ok;
  let array = C.ArrayBuilder.finish builder in
  Stdio.printf "%s\n%!" (C.Array.to_string array);
  let array = Option.value_exn (C.Int32Array.of_gobject array) in
  let v = C.Int32Array.get_value array (Int64.of_int 3) in
  Stdio.printf "%d\n%!" (Int32.to_int_exn v)

let array () = A.of_list [ 1.; 2.; 3. ] Double |> A.to_string |> Stdio.printf "%s\n%!"

let read () =
  let input_stream = C.MemoryMappedInputStream.new_ "/tmp/a.csv" in
  let csv_reader = C.CSVReader.new_ input_stream in
  let table = C.CSVReader.read csv_reader in
  Stdio.printf "%s\n%!" (C.Table.to_string table)

let () =
  hello_world ();
  build ();
  array ();
  read ()
