open! Core_kernel

module type Intf = sig
  type t
  type elem

  val create : unit -> t
  val append : t -> elem -> unit
  val append_null : ?n:int -> t -> unit
  val append_opt : t -> elem option -> unit
  val length : t -> int
  val null_count : t -> int
end

module Double : sig
  include Intf with type elem := float and type t = Wrapper.DoubleBuilder.t
end

module Int64 : sig
  include Intf with type elem := Int64.t and type t = Wrapper.Int64Builder.t
end

module NativeInt : sig
  include Intf with type elem := int and type t = Wrapper.Int64Builder.t
end

module String : sig
  include Intf with type elem := string and type t = Wrapper.StringBuilder.t
end

val make_table : (string * Wrapper.Builder.t) list -> Table.t

module F : sig
  type ('a, 'row, 'elem) col =
    ?name:string -> ('a, 'row, 'elem) Field.t_with_perm -> 'row array -> Writer.col list

  val col_multi
    :  ?name:string
    -> ('a, 'b, 'c) Fieldslib.Field.t_with_perm
    -> f:('c array -> name:string -> Writer.col list)
    -> 'b array
    -> Writer.col list

  val col
    :  ?name:string
    -> ('a, 'b, 'c) Fieldslib.Field.t_with_perm
    -> f:('c array -> name:string -> Writer.col)
    -> 'b array
    -> Writer.col list

  val i64 : ('a, 'row, int) col
  val i64_opt : ('a, 'row, int option) col
  val f64 : ('a, 'row, float) col
  val f64_opt : ('a, 'row, float option) col
  val str : ('a, 'row, string) col
  val str_opt : ('a, 'row, string option) col
  val date : ('a, 'row, Date.t) col
  val date_opt : ('a, 'row, Date.t option) col
  val time_ns : ('a, 'row, Time_ns.t) col
  val time_ns_opt : ('a, 'row, Time_ns.t option) col
  val array_to_table : ('row array -> Writer.col list) list -> 'row array -> Table.t
end

module type Row_intf = sig
  type row

  val array_to_table : row array -> Table.t
end

module Row (R : Row_intf) : sig
  type t
  type row = R.row

  val create : unit -> t
  val append : t -> row -> unit
  val length : t -> int
  val reset : t -> unit
  val to_table : t -> Table.t
end
