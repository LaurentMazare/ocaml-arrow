open Base
module P = Wrapper.Parquet_reader

type t = P.t

let create = P.create
let next = P.next
let close = P.close

let iter_batches ?use_threads ?column_idxs ?mmap filename ~f =
  let t = P.create ?use_threads ?column_idxs ?mmap filename in
  Exn.protect
    ~finally:(fun () -> close t)
    ~f:(fun () ->
      let rec loop_read () =
        match next t with
        | None -> ()
        | Some table ->
          f table;
          loop_read ()
      in
      loop_read ())

let fold_batches ?use_threads ?column_idxs ?mmap filename ~init ~f =
  let t = P.create ?use_threads ?column_idxs ?mmap filename in
  Exn.protect
    ~finally:(fun () -> close t)
    ~f:(fun () ->
      let rec loop_read acc =
        match next t with
        | None -> acc
        | Some table -> f acc table |> loop_read
      in
      loop_read init)

let schema = P.schema
let schema_and_num_rows = P.schema_and_num_rows
let table = P.table
