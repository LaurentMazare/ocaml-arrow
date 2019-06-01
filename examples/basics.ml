let () =
  let arr = Arrow_core.Wrapper.C.null_array_new (Int64.of_int 42) in
  let len = Arrow_core.Wrapper.C.array_get_length arr in
  Stdio.printf "Hello World! %d\n%!" (Int64.to_int len);
  Arrow_core.Wrapper.C.object_unref arr
