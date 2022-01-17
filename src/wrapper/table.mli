include module type of Wrapper.Table with type t = Wrapper.Table.t

type _ col_type =
  | Int : int col_type
  | Float : float col_type
  | Utf8 : string col_type
  | Date : Core_kernel.Date.t col_type
  | Time_ns : Core_kernel.Time_ns.t col_type
  | Span_ns : Core_kernel.Time_ns.Span.t col_type
  | Ofday_ns : Core_kernel.Time_ns.Ofday.t col_type
  | Bool : bool col_type

type packed_col =
  | P : 'a col_type * 'a array -> packed_col
  | O : 'a col_type * 'a option array -> packed_col

val create : Wrapper.Writer.col list -> t
val named_col : packed_col -> name:string -> Wrapper.Writer.col
val col : 'a array -> 'a col_type -> name:string -> Wrapper.Writer.col
val col_opt : 'a option array -> 'a col_type -> name:string -> Wrapper.Writer.col
val read : t -> column:Wrapper.Column.column -> 'a col_type -> 'a array
val read_opt : t -> column:Wrapper.Column.column -> 'a col_type -> 'a option array
