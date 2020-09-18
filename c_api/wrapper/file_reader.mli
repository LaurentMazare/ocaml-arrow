val schema : string -> Wrapper.Schema.t

val table
  :  ?columns:[ `indexes of int list | `names of string list ]
  -> string
  -> Wrapper.Table.t
