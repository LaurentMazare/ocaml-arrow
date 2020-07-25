(* A typical use of this module goes as follows:
  {|
    type t =
      { x : int
      ; y : float
      }
    [@@deriving sexp, fields]

    let read =
      F.read (Fields.make_creator ~x:F.i64 ~y:F.f64)

    let ts = read "/path/to/filename.parquet"
  |}
*)

open! Base

type t
type 'v col_ = t -> (int -> 'v) * t
type ('a, 'b, 'c, 'v) col = ('a, 'b, 'c) Field.t_with_perm -> 'v col_

val i64 : ('a, 'b, 'c, int) col
val f64 : ('a, 'b, 'c, float) col
val str : ('a, 'b, 'c, string) col
val const : 'v -> ('a, 'b, 'c, 'v) col
val read : 'v col_ -> string -> 'v list
