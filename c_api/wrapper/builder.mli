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

  type ('a, 'row, 'elem) col_array =
    ?name:string
    -> suffixes:string array
    -> ('a, 'row, 'elem array) Field.t_with_perm
    -> 'row array
    -> Writer.col list

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

  val ignore : ('a, 'row, 'e) col
  val c : 'elem Table.col_type -> ('a, 'row, 'elem) col
  val c_opt : 'elem Table.col_type -> ('a, 'row, 'elem option) col
  val c_array : 'elem Table.col_type -> ('a, 'row, 'elem) col_array
  val c_opt_array : 'elem Table.col_type -> ('a, 'row, 'elem option) col_array
  val array_to_table : ('row array -> Writer.col list) list -> 'row array -> Table.t
end

module type Row_intf = sig
  type row

  val array_to_table : row array -> Table.t
end

module type Row_builder_intf = sig
  type t
  type row

  val create : unit -> t
  val append : t -> row -> unit
  val length : t -> int
  val reset : t -> unit
  val to_table : t -> Table.t
end

module Row (R : Row_intf) : Row_builder_intf with type row = R.row
