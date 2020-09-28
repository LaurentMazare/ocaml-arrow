include module type of Wrapper.Table with type t = Wrapper.Table.t

type _ type_ =
  | Int : int type_
  | Float : float type_
  | Utf8 : string type_
  | Date : Core_kernel.Date.t type_
  | Time_ns : Core_kernel.Time_ns.t type_
  | Bool : bool type_

val read : t -> column:Wrapper.Column.column -> 'a type_ -> 'a array
val read_opt : t -> column:Wrapper.Column.column -> 'a type_ -> 'a option array
