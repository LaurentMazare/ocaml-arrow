module C = Arrow_core.Wrapper_generated

let hello_world () =
  let arr = C.NullArray.new_ (Int64.of_int 42) in
  let len = C.Array.get_length (C.NullArray.parent arr) in
  Stdio.printf "Hello World! %d\n%!" (Int64.to_int len)

(* Similar to arrow-glib/example/build.c *)
let build () =
  let builder = C.Int32ArrayBuilder.new_ () in
  for i = 0 to 10 do
    let ok = C.Int32ArrayBuilder.append_value builder (Int32.of_int i) in
    assert ok
  done;
  let array = C.ArrayBuilder.finish (C.Int32ArrayBuilder.parent builder) in
  Stdio.printf "%s\n%!" (C.Array.to_string array)

let read () =
  let _file = C.MemoryMappedInputStream.new_ "/tmp/a.csv" in
  ()

let () =
  hello_world ();
  build ();
  read ()
