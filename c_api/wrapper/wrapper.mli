open! Base

module Schema : sig
  module Flags : sig
    type t [@@deriving sexp_of]

    val all : t list -> t
    val dictionary_ordered_ : t
    val nullable_ : t
    val map_keys_sorted_ : t
    val dictionary_ordered : t -> bool
    val nullable : t -> bool
    val map_keys_sorted : t -> bool
  end

  type t =
    { format : Datatype.t
    ; name : string
    ; metadata : (string * string) list
    ; flags : Flags.t
    ; children : t list
    }
  [@@deriving sexp_of]
end

module Table : sig
  type t

  val num_rows : t -> int
  val schema : t -> Schema.t
  val read_csv : string -> t
  val read_json : string -> t
  val write_parquet : ?chunk_size:int -> ?compression:Compression.t -> t -> string -> unit
  val write_feather : ?chunk_size:int -> ?compression:Compression.t -> t -> string -> unit
end

module Parquet_reader : sig
  val schema : string -> Schema.t
  val schema_and_num_rows : string -> Schema.t * int
  val table : string -> column_idxs:int list -> Table.t
end

module Feather_reader : sig
  val schema : string -> Schema.t
  val table : string -> column_idxs:int list -> Table.t
end

module Column : sig
  type column =
    [ `Index of int
    | `Name of string
    ]

  val read_i64_ba
    :  Table.t
    -> column:column
    -> (int64, Bigarray.int64_elt, Bigarray.c_layout) Bigarray.Array1.t

  val read_f64_ba
    :  Table.t
    -> column:column
    -> (float, Bigarray.float64_elt, Bigarray.c_layout) Bigarray.Array1.t

  val read_utf8 : Table.t -> column:column -> string array
  val read_date : Table.t -> column:column -> Core_kernel.Date.t array
  val read_time_ns : Table.t -> column:column -> Core_kernel.Time_ns.t array
  val read_bitset : Table.t -> column:column -> Valid.t

  val read_i64_ba_opt
    :  Table.t
    -> column:column
    -> (int64, Bigarray.int64_elt, Bigarray.c_layout) Bigarray.Array1.t * Valid.t

  val read_f64_ba_opt
    :  Table.t
    -> column:column
    -> (float, Bigarray.float64_elt, Bigarray.c_layout) Bigarray.Array1.t * Valid.t

  val read_utf8_opt : Table.t -> column:column -> string option array
  val read_date_opt : Table.t -> column:column -> Core_kernel.Date.t option array
  val read_time_ns_opt : Table.t -> column:column -> Core_kernel.Time_ns.t option array
end

module Writer : sig
  type col

  val int64_ba
    :  (int64, Bigarray.int64_elt, Bigarray.c_layout) Bigarray.Array1.t
    -> name:string
    -> col

  val int64_ba_opt
    :  (int64, Bigarray.int64_elt, Bigarray.c_layout) Bigarray.Array1.t
    -> Valid.t
    -> name:string
    -> col

  val float64_ba
    :  (float, Bigarray.float64_elt, Bigarray.c_layout) Bigarray.Array1.t
    -> name:string
    -> col

  val float64_ba_opt
    :  (float, Bigarray.float64_elt, Bigarray.c_layout) Bigarray.Array1.t
    -> Valid.t
    -> name:string
    -> col

  val utf8 : string array -> name:string -> col
  val utf8_opt : string option array -> name:string -> col
  val date : Core_kernel.Date.t array -> name:string -> col
  val date_opt : Core_kernel.Date.t option array -> name:string -> col
  val time_ns : Core_kernel.Time_ns.t array -> name:string -> col
  val time_ns_opt : Core_kernel.Time_ns.t option array -> name:string -> col
  val bitset : Valid.t -> name:string -> col

  val write
    :  ?chunk_size:int
    -> ?compression:Compression.t
    -> string
    -> cols:col list
    -> unit
end
