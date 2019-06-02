module C = Arrow_core.Wrapper.C

let hello_world () =
  let arr = C.NullArray.new_ (Int64.of_int 42) in
  let len = C.Array.get_length arr in
  Stdio.printf "Hello World! %d\n%!" (Int64.to_int len);
  C.object_unref arr

(* Similar to arrow-glib/example/build.c *)
let build () =
  let builder = C.Int32ArrayBuilder.new_ () in
  (* let array = C.ArrayBuilder.finish builder in *)
  ignore (builder, ())

let () =
  hello_world ();
  build ()
