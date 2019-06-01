let () =
  let fmt file = Format.formatter_of_out_channel (open_out file) in
  let fmt_c = fmt "arrow_stubs.c" in
  Format.fprintf fmt_c "#include <arrow-glib/arrow-glib.h>@.";
  Cstubs.write_c fmt_c ~prefix:"caml_" (module Arrow_bindings.C);
  let fmt_ml = fmt "arrow_generated.ml" in
  Cstubs.write_ml fmt_ml ~prefix:"caml_" (module Arrow_bindings.C);
  flush_all ()
