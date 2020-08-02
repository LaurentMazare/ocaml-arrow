type t

val create_all_valid : int -> t
val get : t -> int -> bool
val set : t -> int -> bool -> unit
