type t

val read_csv : string -> t
val to_string : t -> string
val num_columns : t -> int
val num_rows : t -> int
val column_names : t -> string list
