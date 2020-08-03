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

module Reader : sig
  type t

  val read : string -> t
  val close : t -> unit
  val num_rows : t -> int
  val schema : t -> Schema.t
  val with_file : string -> f:(t -> 'a) -> 'a
end

module Column : sig
  val read_i64_ba
    :  Reader.t
    -> column_idx:int
    -> (int64, Bigarray.int64_elt, Bigarray.c_layout) Bigarray.Array1.t

  val read_f64_ba
    :  Reader.t
    -> column_idx:int
    -> (float, Bigarray.float64_elt, Bigarray.c_layout) Bigarray.Array1.t

  val read_utf8 : Reader.t -> column_idx:int -> string array
  val read_date : Reader.t -> column_idx:int -> Core_kernel.Date.t array
  val read_time_ns : Reader.t -> column_idx:int -> Core_kernel.Time_ns.t array

  val read_i64_ba_opt
    :  Reader.t
    -> column_idx:int
    -> (int64, Bigarray.int64_elt, Bigarray.c_layout) Bigarray.Array1.t * Valid.t

  val read_f64_ba_opt
    :  Reader.t
    -> column_idx:int
    -> (float, Bigarray.float64_elt, Bigarray.c_layout) Bigarray.Array1.t * Valid.t

  val read_utf8_opt : Reader.t -> column_idx:int -> string option array
  val read_date_opt : Reader.t -> column_idx:int -> Core_kernel.Date.t option array
  val read_time_ns_opt : Reader.t -> column_idx:int -> Core_kernel.Time_ns.t option array
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
  val date : Core_kernel.Date.t array -> name:string -> col
  val date_opt : Core_kernel.Date.t option array -> name:string -> col
  val time_ns : Core_kernel.Time_ns.t array -> name:string -> col
  val time_ns_opt : Core_kernel.Time_ns.t option array -> name:string -> col

  val write
    :  ?chunk_size:int
    -> ?compression:Compression.t
    -> string
    -> cols:col list
    -> unit
end
