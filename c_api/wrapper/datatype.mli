open! Base

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

val of_cstring : string -> t
