open Base

type ('a, 'b) t

val of_list : 'a list -> ('a, 'b) Data_type.t -> ('a, 'b) t
val length : _ t -> int
val to_string : _ t -> string
val get : (_, 'a) t -> int -> 'a
val slice : ('a, 'b) t -> start:int -> length:int -> ('a, 'b) t
val data : (_, _) t -> Arrow_core.Wrapper.Array.t
val of_data : Arrow_core.Wrapper.Array.t -> ('a, 'b) Data_type.t -> ('a, 'b) t Or_error.t
val of_data_exn : Arrow_core.Wrapper.Array.t -> ('a, 'b) Data_type.t -> ('a, 'b) t

type packed = P : _ t -> packed

val pack : _ t -> packed
val unpack : packed -> ('a, 'b) Data_type.t -> ('a, 'b) t option
val packed_length : packed -> int
val packed_to_string : packed -> string
val packed_slice : packed -> start:int -> length:int -> packed
val packed_data : packed -> Arrow_core.Wrapper.Array.t
