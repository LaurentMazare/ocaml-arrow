type t

val create : ?use_threads:bool -> ?column_idxs:int list -> string -> t
val next : t -> Table.t option
val close : t -> unit

val iter_batches
  :  ?use_threads:bool
  -> ?column_idxs:int list
  -> string
  -> f:(Table.t -> unit)
  -> unit

val fold_batches
  :  ?use_threads:bool
  -> ?column_idxs:int list
  -> string
  -> init:'a
  -> f:('a -> Table.t -> 'a)
  -> 'a

val schema : string -> Wrapper.Schema.t
val schema_and_num_rows : string -> Wrapper.Schema.t * int

val table
  :  ?only_first:int
  -> ?use_threads:bool
  -> ?column_idxs:int list
  -> string
  -> Table.t
