open! Base
module W = Arrow_core.Wrapper

type t = W.Column.t

let name t = W.Column.get_name t

let of_array array ~name =
  let data_type = W.Array.get_value_data_type array in
  let field = W.Field.new_ name data_type in
  W.Column.new_array field array

let of_chunked_array chunked_array ~name =
  let data_type = W.ChunkedArray.get_value_data_type chunked_array in
  let field = W.Field.new_ name data_type in
  W.Column.new_chunked_array field chunked_array

let slice t ~start ~len =
  W.Column.slice t (Unsigned.UInt64.of_int start) (Unsigned.UInt64.of_int len)

let to_string t = W.Column.to_string t
