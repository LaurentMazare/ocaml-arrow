module C = Arrow_core.Wrapper

let hello_world () =
  let arr = C.NullArray.new_ (Int64.of_int 42) in
  let len = C.Array.get_length arr in
  Stdio.printf "Hello World! %d\n%!" (Int64.to_int len)

(* Similar to arrow-glib/example/build.c *)
let build () =
  let builder = C.Int32ArrayBuilder.new_ () in
  for i = 0 to 10 do
    let ok = C.Int32ArrayBuilder.append_value builder (Int32.of_int i) in
    assert ok
  done;
  let array = C.ArrayBuilder.finish builder in
  Stdio.printf "%s\n%!" (C.Array.to_string array)

let read () =
  let input_stream = C.MemoryMappedInputStream.new_ "/tmp/a.csv" in
  let input_stream = C.MemoryMappedInputStream.parent input_stream in
  let input_stream = C.SeekableInputStream.parent input_stream in
  let csv_read_options = C.CSVReadOptions.new_ () in
  let csv_reader = C.CSVReader.new_ input_stream csv_read_options in
  let table = C.CSVReader.read csv_reader in
  Stdio.printf "%s\n%!" (C.Table.to_string table)

let () =
  hello_world ();
  build ();
  read ()
