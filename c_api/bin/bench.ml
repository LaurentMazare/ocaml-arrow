open Core_kernel
module A = Arrow_c_api

type t =
  | Null
  | Int of int
  | Float of float
  | String of string

let col_readers table =
  let schema = A.Table.schema table in
  List.mapi schema.children ~f:(fun col_idx { name; format; flags; _ } ->
      match format with
      | Int64 ->
        if A.Schema.Flags.nullable flags
        then (
          let ba, valid = A.Column.read_i64_ba_opt table ~column:(`Index col_idx) in
          fun i -> if A.Valid.get valid i then Int (Int64.to_int_exn ba.{i}) else Null)
        else (
          let ba = A.Column.read_i64_ba table ~column:(`Index col_idx) in
          fun i -> Int (Int64.to_int_exn ba.{i}))
      | Float64 ->
        if A.Schema.Flags.nullable flags
        then (
          let ba, valid = A.Column.read_f64_ba_opt table ~column:(`Index col_idx) in
          fun i -> if A.Valid.get valid i then Float ba.{i} else Null)
        else (
          let ba = A.Column.read_f64_ba table ~column:(`Index col_idx) in
          fun i -> Float ba.{i})
      | Utf8_string ->
        if A.Schema.Flags.nullable flags
        then (
          let arr = A.Column.read_utf8_opt table ~column:(`Index col_idx) in
          fun i ->
            match arr.(i) with
            | None -> Null
            | Some str -> String str)
        else (
          let arr = A.Column.read_utf8 table ~column:(`Index col_idx) in
          fun i -> String arr.(i))
      | dt -> raise_s [%message "unsupported column type" name (dt : A.Datatype.t)])

let () =
  let filename =
    match Caml.Sys.argv with
    | [| _exe; filename |] -> filename
    | _ -> Printf.failwithf "usage: %s file.parquet" Caml.Sys.argv.(0) ()
  in
  A.Parquet_reader.iter_batches filename ~f:(fun table ->
      let num_rows = A.Table.num_rows table in
      let col_readers = col_readers table in
      for row_idx = 0 to num_rows - 1 do
        let _value = List.map col_readers ~f:(fun col_reader -> col_reader row_idx) in
        ignore (_value : _ list)
      done;
      Stdio.printf "read batch with %d rows\n%!" num_rows)
