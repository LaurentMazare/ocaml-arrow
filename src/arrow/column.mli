type t = Arrow_core.Wrapper.Column.t

val of_array : Arrow_core.Wrapper.Array.t -> name:string -> t
val of_chunked_array : Arrow_core.Wrapper.ChunkedArray.t -> name:string -> t
val name : t -> string
val slice : t -> start:int -> len:int -> t
val to_string : t -> string
