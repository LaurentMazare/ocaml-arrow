open! Base
module W = Arrow_core.Wrapper

type t = W.Column.t

let name t = W.Column.get_name t

let slice t ~start ~len =
  W.Column.slice t (Unsigned.UInt64.of_int start) (Unsigned.UInt64.of_int len)

let to_string t = W.Column.to_string t
