open! Base

let is_feather = String.is_suffix ~suffix:".feather"

let schema filename =
  if is_feather filename
  then Wrapper.Feather_reader.schema filename
  else Wrapper.Parquet_reader.schema filename

let table ?column_idxs filename =
  if is_feather filename
  then Wrapper.Feather_reader.table ?column_idxs filename
  else Wrapper.Parquet_reader.table ?column_idxs filename
