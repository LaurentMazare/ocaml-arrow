open Core_kernel
module A = Arrow_c_api

let debug = false

type t =
  | Null
  | Int of int
  | Float of float
  | String of string
[@@deriving sexp]

let col_readers table =
  let schema = A.Table.schema table in
  List.mapi schema.children ~f:(fun col_idx { name; format; _ } ->
      match format with
      | Int64 ->
        (match A.Column.fast_read table col_idx with
        | Int64 arr -> fun i -> Int (Int64.to_int_exn arr.{i})
        | Int64_option (ba, valid) ->
          let valid = A.Valid.of_bigarray valid ~length:(Bigarray.Array1.dim ba) in
          fun i -> if A.Valid.get valid i then Int (Int64.to_int_exn ba.{i}) else Null
        | _ -> assert false)
      | Float64 ->
        (match A.Column.fast_read table col_idx with
        | Double arr -> fun i -> Float arr.{i}
        | Double_option (ba, valid) ->
          let valid = A.Valid.of_bigarray valid ~length:(Bigarray.Array1.dim ba) in
          fun i -> if A.Valid.get valid i then Float ba.{i} else Null
        | _ -> assert false)
      | Utf8_string ->
        (match A.Column.fast_read table col_idx with
        | String arr -> fun i -> String arr.(i)
        | String_option arr ->
          fun i ->
            (match arr.(i) with
            | None -> Null
            | Some str -> String str)
        | _ -> assert false)
      | dt -> raise_s [%message "unsupported column type" name (dt : A.Datatype.t)])

let () =
  let filename =
    match Caml.Sys.argv with
    | [| _exe; filename |] -> filename
    | _ -> Printf.failwithf "usage: %s file.parquet" Caml.Sys.argv.(0) ()
  in
  let prev_time = ref (Time_ns.now ()) in
  A.Parquet_reader.iter_batches filename ~batch_size:8192 ~f:(fun table ->
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
