open! Core_kernel
open! Arrow_c_api

let all_cols = ref []

let run2 () =
  let tables =
    List.init 5 ~f:(fun i ->
        let cols =
          [ Wrapper.Writer.utf8 [| "v1"; "v2"; "v3" |] ~name:"foo"
          ; Wrapper.Writer.int [| i; 5 * i; 10 * i |] ~name:"bar"
          ; Wrapper.Writer.int_opt [| Some ((i * 2) + 1); None; None |] ~name:"baz"
          ]
        in
        Stdio.printf "pre-create\n%!";
        (* if false then all_cols := !all_cols @ cols; *)
        Wrapper.Writer.create_table ~cols)
  in
  Stdio.printf "concat\n%!";
  (* let table = Wrapper.Table.concatenate tables in *)
  Wrapper.Table.write_parquet (List.last_exn tables) "/tmp/foo.parquet";
  (*  let _col = Wrapper.Column.read_utf8 table ~column:(`Name "foo") in *)
  Stdio.printf ">> %d %d\n%!" (List.length !all_cols) (List.length tables);
  ()

let () = run2 ()
