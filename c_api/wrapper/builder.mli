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

  val c_ignore : ('a, 'row, 'e) col
  val c_flatten : ('elem array -> Writer.col list) list -> ('a, 'row, 'elem) col
  val c : 'elem Table.col_type -> ('a, 'row, 'elem) col
  val c_opt : 'elem Table.col_type -> ('a, 'row, 'elem option) col
  val c_array : 'elem Table.col_type -> ('a, 'row, 'elem) col_array
  val c_opt_array : 'elem Table.col_type -> ('a, 'row, 'elem option) col_array
  val array_to_table : ('row array -> Writer.col list) list -> 'row array -> Table.t
end

module C : sig
  type ('row, 'elem, 'col_type) col =
    { name : string
    ; get : 'row -> 'elem
    ; col_type : 'col_type Table.col_type
    }

  type 'row packed_col =
    | P : ('row, 'elem, 'elem) col -> 'row packed_col
    | O : ('row, 'elem option, 'elem) col -> 'row packed_col

  type 'row packed_cols = 'row packed_col list

  val c : 'a Table.col_type -> ('b, 'c, 'a) Field.t_with_perm -> 'c packed_cols
  val c_opt : 'a Table.col_type -> ('b, 'c, 'a option) Field.t_with_perm -> 'c packed_cols

  val c_map
    :  'a Table.col_type
    -> ('b, 'c, 'd) Field.t_with_perm
    -> f:('d -> 'a)
    -> 'c packed_cols

  val c_map_opt
    :  'a Table.col_type
    -> ('b, 'c, 'd) Field.t_with_perm
    -> f:('d -> 'a option)
    -> 'c packed_cols

  val c_ignore : ('b, 'c, 'a) Field.t_with_perm -> 'c packed_cols

  val c_flatten
    :  ?rename:[ `fn of string -> string | `keep | `prefix ]
    -> ('b, 'c, 'a) Field.t_with_perm
    -> 'a packed_cols
    -> 'c packed_cols

  val array_to_table : 'a packed_cols -> 'a array -> Table.t
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
