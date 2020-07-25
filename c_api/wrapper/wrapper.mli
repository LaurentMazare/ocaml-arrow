open! Base

module Format : sig
  type t =
    | Null
    | Boolean
    | Int8
    | Uint8
    | Int16
    | Uint16
    | Int32
    | Uint32
    | Int64
    | Uint64
    | Float16
    | Float32
    | Float64
    | Binary
    | Large_binary
    | Utf8_string
    | Large_utf8_string
    | Decimal128 of
        { precision : int
        ; scale : int
        }
    | Fixed_width_binary of { bytes : int }
    | Date32 of [ `days ]
    | Date64 of [ `milliseconds ]
    | Time32 of [ `seconds | `milliseconds ]
    | Time64 of [ `microseconds | `nanoseconds ]
    | Timestamp of
        { precision : [ `seconds | `milliseconds | `microseconds | `nanoseconds ]
        ; timezone : string
        }
    | Duration of [ `seconds | `milliseconds | `microseconds | `nanoseconds ]
    | Interval of [ `months | `days_time ]
    | Struct
    | Map
    | Unknown of string
  [@@deriving sexp]
end

module Reader : sig
  type t

  val read : string -> t
  val close : t -> unit
  val with_file : string -> f:(t -> 'a) -> 'a
end

module Schema : sig
  type t =
    { format : Format.t
    ; name : string
    ; metadata : (string * string) list
    ; flags : int
    ; children : t list
    }
  [@@deriving sexp]

  val get : Reader.t -> t
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
end
