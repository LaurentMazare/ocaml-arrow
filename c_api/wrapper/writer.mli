include module type of Wrapper.Writer with type col = Wrapper.Writer.col

val col : 'a array -> 'a Table.col_type -> name:string -> col
val col_opt : 'a option array -> 'a Table.col_type -> name:string -> col
