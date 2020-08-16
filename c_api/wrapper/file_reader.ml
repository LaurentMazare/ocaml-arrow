open! Base

let unknown_suffix filename =
  Printf.failwithf
    "cannot infer the file format from suffix %s (try using .parquet as a suffix)"
    filename
    ()

let schema filename =
  match String.rsplit2 filename ~on:'.' with
  | Some (_, "csv") -> Wrapper.Table.read_csv filename |> Wrapper.Table.schema
  | Some (_, "json") -> Wrapper.Table.read_json filename |> Wrapper.Table.schema
  | Some (_, "feather") -> Wrapper.Feather_reader.schema filename
  | Some (_, "parquet") -> Wrapper.Parquet_reader.schema filename
  | Some _ | None -> unknown_suffix filename

let table ?column_idxs filename =
  match String.rsplit2 filename ~on:'.' with
  | Some (_, "csv") -> Wrapper.Table.read_csv filename
  | Some (_, "json") -> Wrapper.Table.read_json filename
  | Some (_, "feather") -> Wrapper.Feather_reader.table ?column_idxs filename
  | Some (_, "parquet") -> Wrapper.Parquet_reader.table ?column_idxs filename
  | Some _ | None -> unknown_suffix filename
