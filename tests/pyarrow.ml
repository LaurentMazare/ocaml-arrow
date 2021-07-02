open! Core_kernel
open! Arrow_c_api

let%expect_test _ =
  Py.initialize ();
  let go = function
    | [| arg |] -> arg
    | _ -> failwith "only a single argument is expected"
  in
  let mdl = Py.Import.add_module "mdl" in
  Py.Module.set mdl "go" (Py.Callable.of_function go);
  let _none =
    Py.Run.eval
      ~start:Py.File
      {|
# import pyarrow as pa
import mdl
print(mdl.go("here"))
  |}
  in
  [%expect {|
    |}]
