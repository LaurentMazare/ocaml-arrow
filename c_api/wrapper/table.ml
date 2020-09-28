open Base
include Wrapper.Table

type _ type_ =
  | Int : int type_
  | Float : float type_
  | Utf8 : string type_
  | Date : Core_kernel.Date.t type_
  | Time_ns : Core_kernel.Time_ns.t type_
  | Bool : bool type_

let read (type a) t ~column (type_ : a type_) : a array =
  match type_ with
  | Int -> Wrapper.Column.read_int t ~column
  | Float -> Wrapper.Column.read_float t ~column
  | Utf8 -> Wrapper.Column.read_utf8 t ~column
  | Date -> Wrapper.Column.read_date t ~column
  | Time_ns -> Wrapper.Column.read_time_ns t ~column
  | Bool ->
    let bs = Wrapper.Column.read_bitset t ~column in
    Array.init (Valid.length bs) ~f:(Valid.get bs)

let read_opt (type a) t ~column (type_ : a type_) : a option array =
  match type_ with
  | Int -> Wrapper.Column.read_int_opt t ~column
  | Float -> Wrapper.Column.read_float_opt t ~column
  | Utf8 -> Wrapper.Column.read_utf8_opt t ~column
  | Date -> Wrapper.Column.read_date_opt t ~column
  | Time_ns -> Wrapper.Column.read_time_ns_opt t ~column
  | Bool ->
    let bs, valid = Wrapper.Column.read_bitset_opt t ~column in
    Array.init (Valid.length bs) ~f:(fun i ->
        if Valid.get valid i then Valid.get bs i |> Option.some else None)
