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

module ChunkedArray : sig
  type t
end

module Table : sig
  type t

  val concatenate : t list -> t
  val slice : t -> offset:int -> length:int -> t
  val num_rows : t -> int
  val schema : t -> Schema.t
  val read_csv : string -> t
  val read_json : string -> t
  val write_parquet : ?chunk_size:int -> ?compression:Compression.t -> t -> string -> unit
  val write_feather : ?chunk_size:int -> ?compression:Compression.t -> t -> string -> unit
  val to_string_debug : t -> string
  val add_column : t -> string -> ChunkedArray.t -> t
  val get_column : t -> string -> ChunkedArray.t
  val add_all_columns : t -> t -> t
end

module Parquet_reader : sig
  type t

  val create
    :  ?use_threads:bool
    -> ?column_idxs:int list
    -> ?mmap:bool
    -> ?buffer_size:int
    -> ?batch_size:int
    -> string
    -> t

  val next : t -> Table.t option
  val close : t -> unit
  val schema : string -> Schema.t
  val schema_and_num_rows : string -> Schema.t * int

  val table
    :  ?only_first:int
    -> ?use_threads:bool
    -> ?column_idxs:int list
    -> string
    -> Table.t
end

module Feather_reader : sig
  val schema : string -> Schema.t
  val table : ?column_idxs:int list -> string -> Table.t
end

module Column : sig
  type column =
    [ `Index of int
    | `Name of string
    ]

  val read_i32_ba
    :  Table.t
    -> column:column
    -> (int32, Bigarray.int32_elt, Bigarray.c_layout) Bigarray.Array1.t

  val read_i64_ba
    :  Table.t
    -> column:column
    -> (int64, Bigarray.int64_elt, Bigarray.c_layout) Bigarray.Array1.t

  val read_f64_ba
    :  Table.t
    -> column:column
    -> (float, Bigarray.float64_elt, Bigarray.c_layout) Bigarray.Array1.t

  val read_f32_ba
    :  Table.t
    -> column:column
    -> (float, Bigarray.float32_elt, Bigarray.c_layout) Bigarray.Array1.t

  val read_int : Table.t -> column:column -> int array
  val read_int32 : Table.t -> column:column -> Int32.t array
  val read_float : Table.t -> column:column -> float array
  val read_utf8 : Table.t -> column:column -> string array
  val read_date : Table.t -> column:column -> Core_kernel.Date.t array
  val read_time_ns : Table.t -> column:column -> Core_kernel.Time_ns.t array
  val read_ofday_ns : Table.t -> column:column -> Core_kernel.Time_ns.Ofday.t array
  val read_span_ns : Table.t -> column:column -> Core_kernel.Time_ns.Span.t array
  val read_bitset : Table.t -> column:column -> Valid.t
  val read_bitset_opt : Table.t -> column:column -> Valid.t * Valid.t

  val read_i32_ba_opt
    :  Table.t
    -> column:column
    -> (int32, Bigarray.int32_elt, Bigarray.c_layout) Bigarray.Array1.t * Valid.t

  val read_i64_ba_opt
    :  Table.t
    -> column:column
    -> (int64, Bigarray.int64_elt, Bigarray.c_layout) Bigarray.Array1.t * Valid.t

  val read_f64_ba_opt
    :  Table.t
    -> column:column
    -> (float, Bigarray.float64_elt, Bigarray.c_layout) Bigarray.Array1.t * Valid.t

  val read_f32_ba_opt
    :  Table.t
    -> column:column
    -> (float, Bigarray.float32_elt, Bigarray.c_layout) Bigarray.Array1.t * Valid.t

  val read_int_opt : Table.t -> column:column -> int option array
  val read_int32_opt : Table.t -> column:column -> Int32.t option array
  val read_float_opt : Table.t -> column:column -> float option array
  val read_utf8_opt : Table.t -> column:column -> string option array
  val read_date_opt : Table.t -> column:column -> Core_kernel.Date.t option array
  val read_time_ns_opt : Table.t -> column:column -> Core_kernel.Time_ns.t option array

  val read_ofday_ns_opt
    :  Table.t
    -> column:column
    -> Core_kernel.Time_ns.Ofday.t option array

  val read_span_ns_opt
    :  Table.t
    -> column:column
    -> Core_kernel.Time_ns.Span.t option array

  type t =
    | Unsupported_type
    | String of string array
    | String_option of string option array
    | Int64 of (int64, Bigarray.int64_elt, Bigarray.c_layout) Bigarray.Array1.t
    | Int64_option of
        (int64, Bigarray.int64_elt, Bigarray.c_layout) Bigarray.Array1.t * Valid.ba
    | Double of (float, Bigarray.float64_elt, Bigarray.c_layout) Bigarray.Array1.t
    | Double_option of
        (float, Bigarray.float64_elt, Bigarray.c_layout) Bigarray.Array1.t * Valid.ba
  [@@deriving sexp_of]

  val fast_read : Table.t -> int -> t
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

  val int : int array -> name:string -> col
  val int_opt : int option array -> name:string -> col

  val float64_ba
    :  (float, Bigarray.float64_elt, Bigarray.c_layout) Bigarray.Array1.t
    -> name:string
    -> col

  val float64_ba_opt
    :  (float, Bigarray.float64_elt, Bigarray.c_layout) Bigarray.Array1.t
    -> Valid.t
    -> name:string
    -> col

  val float : float array -> name:string -> col
  val float_opt : float option array -> name:string -> col
  val utf8 : string array -> name:string -> col
  val utf8_opt : string option array -> name:string -> col
  val date : Core_kernel.Date.t array -> name:string -> col
  val date_opt : Core_kernel.Date.t option array -> name:string -> col

  (* Timestamps are encoded in a "timezone naive" way, and implicitely using
     GMT. *)
  val time_ns : Core_kernel.Time_ns.t array -> name:string -> col
  val time_ns_opt : Core_kernel.Time_ns.t option array -> name:string -> col
  val ofday_ns : Core_kernel.Time_ns.Ofday.t array -> name:string -> col
  val ofday_ns_opt : Core_kernel.Time_ns.Ofday.t option array -> name:string -> col
  val span_ns : Core_kernel.Time_ns.Span.t array -> name:string -> col
  val span_ns_opt : Core_kernel.Time_ns.Span.t option array -> name:string -> col
  val bitset : Valid.t -> name:string -> col
  val bitset_opt : Valid.t -> valid:Valid.t -> name:string -> col

  val write
    :  ?chunk_size:int
    -> ?compression:Compression.t
    -> string
    -> cols:col list
    -> unit

  val create_table : cols:col list -> Table.t
end

module DoubleBuilder : sig
  type t

  val create : unit -> t
  val append : t -> float -> unit
  val append_null : ?n:int -> t -> unit
  val length : t -> Int64.t
  val null_count : t -> Int64.t
end

module Int32Builder : sig
  type t

  val create : unit -> t
  val append : t -> Int32.t -> unit
  val append_null : ?n:int -> t -> unit
  val length : t -> Int64.t
  val null_count : t -> Int64.t
end

module Int64Builder : sig
  type t

  val create : unit -> t
  val append : t -> Int64.t -> unit
  val append_null : ?n:int -> t -> unit
  val length : t -> Int64.t
  val null_count : t -> Int64.t
end

module StringBuilder : sig
  type t

  val create : unit -> t
  val append : t -> string -> unit
  val append_null : ?n:int -> t -> unit
  val length : t -> Int64.t
  val null_count : t -> Int64.t
end

module Builder : sig
  type t =
    | Double of DoubleBuilder.t
    | Int32 of Int32Builder.t
    | Int64 of Int64Builder.t
    | String of StringBuilder.t

  val make_table : (string * t) list -> Table.t
end
