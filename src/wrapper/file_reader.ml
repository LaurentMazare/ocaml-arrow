open! Base

let unknown_suffix filename =
  Printf.failwithf
    "cannot infer the file format from suffix %s (supported suffixes are \
     csv/json/feather/parquet)"
    filename
    ()

let schema filename =
  match String.rsplit2 filename ~on:'.' with
  | Some (_, "csv") -> Table.read_csv filename |> Table.schema
  | Some (_, "json") -> Table.read_json filename |> Table.schema
  | Some (_, "feather") -> Wrapper.Feather_reader.schema filename
  | Some (_, "parquet") -> Wrapper.Parquet_reader.schema filename
  | Some _ | None -> unknown_suffix filename

let indexes columns ~filename =
  match columns with
  | `indexes indexes -> indexes
  | `names col_names ->
    let schema = schema filename in
    let col_names = Set.of_list (module String) col_names in
    List.filter_mapi schema.children ~f:(fun i schema ->
        let col_name = schema.Wrapper.Schema.name in
        if Set.mem col_names col_name then Some i else None)

let table ?columns filename =
  match String.rsplit2 filename ~on:'.' with
  | Some (_, "csv") -> Table.read_csv filename
  | Some (_, "json") -> Table.read_json filename
  | Some (_, "feather") ->
    let column_idxs = Option.map columns ~f:(indexes ~filename) in
    Wrapper.Feather_reader.table ?column_idxs filename
  | Some (_, "parquet") ->
    let column_idxs = Option.map columns ~f:(indexes ~filename) in
    Wrapper.Parquet_reader.table ?column_idxs filename
  | Some _ | None -> unknown_suffix filename
