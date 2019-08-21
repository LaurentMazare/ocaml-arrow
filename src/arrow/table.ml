open Base
module W = Arrow_core.Wrapper

type t = W.Table.t

let read_csv filename =
  let input_stream = W.MemoryMappedInputStream.new_ filename in
  let csv_reader = W.CSVReader.new_ input_stream in
  W.CSVReader.read csv_reader

let read_feather filename =
  let input_stream = W.MemoryMappedInputStream.new_ filename in
  let csv_reader = W.FeatherFileReader.new_ input_stream in
  W.FeatherFileReader.read csv_reader

let write_feather t ?(append = false) filename =
  let input_stream = W.FileOutputStream.new_ filename append in
  let csv_reader = W.FeatherFileWriter.new_ input_stream in
  let ok = W.FeatherFileWriter.write csv_reader t in
  if not ok then Printf.failwithf "writing %s failed" filename ()

let to_string = W.Table.to_string
let num_columns t = W.Table.get_n_columns t |> Unsigned.UInt32.to_int
let num_rows t = W.Table.get_n_rows t |> Unsigned.UInt64.to_int

let column_names t =
  List.init (num_columns t) ~f:(fun i ->
      let column = W.Table.get_column t (Unsigned.UInt32.of_int i) in
      W.Column.get_name column)

let column t ~index = W.Table.get_column t (Unsigned.UInt32.of_int index)
let add_column t col ~index = W.Table.add_column t (Unsigned.UInt32.of_int index) col
let remove_column t ~index = W.Table.remove_column t (Unsigned.UInt32.of_int index)
let slice t ~start ~len = W.Table.slice t (Int64.of_int start) (Int64.of_int len)
