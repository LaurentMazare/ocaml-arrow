type ba = (int, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t
type t

val of_bigarray : ba -> length:int -> t
val create_all_valid : int -> t
val length : t -> int
val get : t -> int -> bool
val set : t -> int -> bool -> unit
val bigarray : t -> ba
val num_true : t -> int
val num_false : t -> int
