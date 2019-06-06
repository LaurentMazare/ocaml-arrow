open Base
module W = Arrow_core.Wrapper

type t = W.Table.t

let read_csv filename =
  let input_stream = W.MemoryMappedInputStream.new_ filename in
  let csv_reader = W.CSVReader.new_ input_stream in
  W.CSVReader.read csv_reader

let to_string = W.Table.to_string
let num_columns t = W.Table.get_n_columns t |> Unsigned.UInt32.to_int
let num_rows t = W.Table.get_n_rows t |> Unsigned.UInt64.to_int

let column_names t =
  List.init (num_columns t) ~f:(fun i ->
      let column = W.Table.get_column t (Unsigned.UInt32.of_int i) in
      W.Column.get_name column)
