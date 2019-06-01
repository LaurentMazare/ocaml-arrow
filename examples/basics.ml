let () =
  Stdio.printf "Hello World!\n%!";
  let arr = Arrow_core.Wrapper.C.null_array_new (Int64.of_int 42) in
  Arrow_core.Wrapper.C.object_unref arr
