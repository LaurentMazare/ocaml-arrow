open Core_kernel
module A = Arrow_c_api

let fast = true
let debug = false

type t =
  | Null
  | Int of int
  | Float of float
  | String of string
[@@deriving sexp]

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
        then
          if fast
          then (
            match A.Column.experimental_fast_read table col_idx with
            | String arr -> fun i -> String arr.(i)
            | String_option arr ->
              fun i ->
                (match arr.(i) with
                | None -> Null
                | Some str -> String str)
            | _ -> assert false)
          else (
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
  let prev_time = ref (Time_ns.now ()) in
  A.Parquet_reader.iter_batches filename ~f:(fun table ->
      let num_rows = A.Table.num_rows table in
      let col_readers = col_readers table in
      for row_idx = 0 to num_rows - 1 do
        let values = List.map col_readers ~f:(fun col_reader -> col_reader row_idx) in
        if debug then print_s ([%sexp_of: t list] values)
      done;
      let now = Time_ns.now () in
      let dt = Time_ns.diff now !prev_time |> Time_ns.Span.to_sec in
      let krows_per_sec = Float.of_int num_rows /. dt /. 1000. in
      prev_time := now;
      Stdio.printf "read batch with %d rows, %.0f krows/sec\n%!" num_rows krows_per_sec)
