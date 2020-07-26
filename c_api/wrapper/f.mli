open! Base

module Reader : sig
  (* A typical use of this module goes as follows:
  {|
    type t =
      { x : int
      ; y : float
      }
    [@@deriving sexp, fields]

    let read =
      F.read (Fields.make_creator ~x:F.i64 ~y:F.f64)

    let ts = read "/path/to/filename.parquet"
  |}
*)

  type t
  type 'v col_ = t -> (int -> 'v) * t
  type ('a, 'b, 'c, 'v) col = ('a, 'b, 'c) Field.t_with_perm -> 'v col_

  val i64 : ('a, 'b, 'c, int) col
  val f64 : ('a, 'b, 'c, float) col
  val str : ('a, 'b, 'c, string) col
  val const : 'v -> ('a, 'b, 'c, 'v) col
  val read : 'v col_ -> string -> 'v list
end

module Writer : sig
  type 'a state = int * (unit -> Wrapper.Writer.col) list * (int -> 'a -> unit)

  val i64 : 'a state -> ('c, 'a, int) Field.t_with_perm -> 'a state
  val f64 : 'a state -> ('c, 'a, float) Field.t_with_perm -> 'a state
  val str : 'a state -> ('c, 'a, string) Field.t_with_perm -> 'a state
  val write : (init:'d state -> 'd state) -> string -> 'd list -> unit
end
