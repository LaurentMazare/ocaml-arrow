module C = Arrow_core.Wrapper_generated

let hello_world () =
  let arr = C.NullArray.new_ (Int64.of_int 42) in
  let len = C.Array.get_length (C.NullArray.parent arr) in
  Stdio.printf "Hello World! %d\n%!" (Int64.to_int len)

(* Similar to arrow-glib/example/build.c *)
let build () =
  let builder = C.Int32ArrayBuilder.new_ () in
  (* let array = C.ArrayBuilder.finish builder in *)
  ignore (builder, ())

let read () =
  let _file = C.MemoryMappedInputStream.new_ "/tmp/a.csv" in
  ()

let () =
  hello_world ();
  build ();
  read ()
