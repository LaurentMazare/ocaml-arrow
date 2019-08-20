(** An arrow table: a table has some named columns for which
    each row has a value.
*)
type t

(** [read_csv filename] reads a csv file as a table. *)
val read_csv : string -> t

(** [read_feather filename] reads a feather file as a table. *)
val read_feather : string -> t

(** [write_feather t ?append filename] writes [t] to a file.
    If [append] is true, the content is appended at the end of the
    file, the default for [append] is [false].
*)
val write_feather : t -> ?append:bool -> string -> unit

(** [to_string t] returns a pretty-printing of table [t]. *)
val to_string : t -> string

(** [num_columns t] returns the number of columns for table [t]. *)
val num_columns : t -> int

(** [num_rows t] returns the number of rows in table [t]. *)
val num_rows : t -> int

(** [column_names t] returns the list of columns defined in table [t].
    The number of element returned is equal to [num_columns t].
*)
val column_names : t -> string list
