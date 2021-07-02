(* A typical use of this module goes as follows:
     {|
       type t =
         { x : int
         ; y : float
         }
       [@@deriving sexp, fields]

       let `read read, `write write =
         F.(read_write_fn (Fields.make_creator ~x:i64 ~y:f64))

       let ts = read "/path/to/filename.parquet"
       let () = write ts "/path/to/another.parquet"
     |}
*)
open! Base

module Reader : sig
  type t = string list
  type 'v col_ = t -> (Wrapper.Table.t * int -> 'v) * t
  type ('a, 'b, 'c, 'v) col = ('a, 'b, 'c) Field.t_with_perm -> 'v col_

  val i64 : ('a, 'b, 'c, int) col
  val f64 : ('a, 'b, 'c, float) col
  val str : ('a, 'b, 'c, string) col
  val stringable : (module Stringable.S with type t = 'd) -> ('a, 'b, 'c, 'd) col
  val date : ('a, 'b, 'c, Core_kernel.Date.t) col
  val time_ns : ('a, 'b, 'c, Core_kernel.Time_ns.t) col
  val bool : ('a, 'b, 'c, bool) col
  val i64_opt : ('a, 'b, 'c, int option) col
  val f64_opt : ('a, 'b, 'c, float option) col
  val str_opt : ('a, 'b, 'c, string option) col
  val bool_opt : ('a, 'b, 'c, bool option) col

  val stringable_opt
    :  (module Stringable.S with type t = 'd)
    -> ('a, 'b, 'c, 'd option) col

  val date_opt : ('a, 'b, 'c, Core_kernel.Date.t option) col
  val time_ns_opt : ('a, 'b, 'c, Core_kernel.Time_ns.t option) col
  val map : ('a, 'b, 'c, 'x) col -> f:('x -> 'y) -> ('a, 'b, 'c, 'y) col
  val read : 'v col_ -> string -> 'v list
end

module Writer : sig
  type 'a state = int * (unit -> Wrapper.Writer.col) list * (int -> 'a -> unit)
  type ('a, 'b, 'c) col = 'a state -> ('b, 'a, 'c) Field.t_with_perm -> 'a state

  val i64 : ('a, 'b, int) col
  val f64 : ('a, 'b, float) col
  val str : ('a, 'b, string) col
  val bool : ('a, 'b, bool) col

  val stringable
    :  (module Stringable.S with type t = 'd)
    -> 'a state
    -> ('c, 'a, 'd) Field.t_with_perm
    -> 'a state

  val date : ('a, 'b, Core_kernel.Date.t) col
  val time_ns : ('a, 'b, Core_kernel.Time_ns.t) col
  val i64_opt : ('a, 'b, int option) col
  val f64_opt : ('a, 'b, float option) col
  val str_opt : ('a, 'b, string option) col
  val date_opt : ('a, 'b, Core_kernel.Date.t option) col
  val time_ns_opt : ('a, 'b, Core_kernel.Time_ns.t option) col
  val bool_opt : ('a, 'b, bool option) col

  val write
    :  (init:'d state -> 'd state)
    -> ?chunk_size:int
    -> ?compression:Compression.t
    -> string
    -> 'd list
    -> unit
end

type 'a t =
  | Read of Reader.t
  | Write of 'a Writer.state

type ('a, 'b, 'c) col =
  ('a, 'b, 'c) Field.t_with_perm -> 'b t -> (Wrapper.Table.t * int -> 'c) * 'b t

val i64 : ('a, 'b, int) col
val f64 : ('a, 'b, float) col
val str : ('a, 'b, string) col
val bool : ('a, 'b, bool) col
val bool_opt : ('a, 'b, bool option) col
val stringable : (module Stringable.S with type t = 'd) -> ('a, 'b, 'd) col
val date : ('a, 'b, Core_kernel.Date.t) col
val time_ns : ('a, 'b, Core_kernel.Time_ns.t) col
val i64_opt : ('a, 'b, int option) col
val f64_opt : ('a, 'b, float option) col
val str_opt : ('a, 'b, string option) col
val date_opt : ('a, 'b, Core_kernel.Date.t option) col
val time_ns_opt : ('a, 'b, Core_kernel.Time_ns.t option) col

val read_write_fn
  :  ('a t -> (Wrapper.Table.t * int -> 'a) * 'a t)
  -> [ `read of string -> 'a list ]
     * [ `write of
         ?chunk_size:int -> ?compression:Compression.t -> string -> 'a list -> unit
       ]
