include module type of Wrapper.Table with type t = Wrapper.Table.t

type _ col_type =
  | Int : int col_type
  | Float : float col_type
  | Utf8 : string col_type
  | Date : Core_kernel.Date.t col_type
  | Time_ns : Core_kernel.Time_ns.t col_type
  | Bool : bool col_type

val read : t -> column:Wrapper.Column.column -> 'a col_type -> 'a array
val read_opt : t -> column:Wrapper.Column.column -> 'a col_type -> 'a option array
