type ('a, 'b) t

val of_list : 'b list -> ('a, 'b) Kind.t -> ('a, 'b) t
val length : _ t -> int
val get : (_, 'a) t -> int -> 'a
val set : (_, 'a) t -> int -> 'a -> unit
val slice : ('a, 'b) t -> start:int -> length:int -> ('a, 'b) t

type packed = P : _ t -> packed

val pack : _ t -> packed
val unpack : packed -> ('a, 'b) Kind.t -> ('a, 'b) t option
val packed_length : packed -> int
val packed_slice : packed -> start:int -> length:int -> packed
