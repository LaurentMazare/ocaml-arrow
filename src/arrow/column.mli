type t = Arrow_core.Wrapper.Column.t

val name : t -> string
val slice : t -> start:int -> len:int -> t
val to_string : t -> string
