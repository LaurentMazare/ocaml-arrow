open Base
include Wrapper.Table

type _ col_type =
  | Int : int col_type
  | Float : float col_type
  | Utf8 : string col_type
  | Date : Core_kernel.Date.t col_type
  | Time_ns : Core_kernel.Time_ns.t col_type
  | Bool : bool col_type

type packed_col =
  | P : 'a col_type * 'a array -> packed_col
  | O : 'a col_type * 'a option array -> packed_col

let col (type a) (data : a array) (type_ : a col_type) ~name =
  let open Writer in
  match type_ with
  | Int -> int data ~name
  | Float -> float data ~name
  | Utf8 -> utf8 data ~name
  | Date -> date data ~name
  | Time_ns -> time_ns data ~name
  | Bool ->
    let bs = Valid.create_all_valid (Array.length data) in
    Array.iteri data ~f:(fun i a -> if not a then Valid.set bs i a);
    bitset bs ~name

let col_opt (type a) (data : a option array) (type_ : a col_type) ~name =
  let open Writer in
  match type_ with
  | Int -> int_opt data ~name
  | Float -> float_opt data ~name
  | Utf8 -> utf8_opt data ~name
  | Date -> date_opt data ~name
  | Time_ns -> time_ns_opt data ~name
  | Bool ->
    let bs = Valid.create_all_valid (Array.length data) in
    let valid = Valid.create_all_valid (Array.length data) in
    Array.iteri data ~f:(fun i a ->
        match a with
        | None -> Valid.set valid i false
        | Some false -> Valid.set bs i false
        | Some true -> ());
    bitset_opt bs ~valid ~name

let named_col packed_col ~name =
  match packed_col with
  | P (typ_, data) -> col data typ_ ~name
  | O (typ_, data) -> col_opt data typ_ ~name

let create cols = Writer.create_table ~cols

let read (type a) t ~column (col_type : a col_type) : a array =
  match col_type with
  | Int -> Wrapper.Column.read_int t ~column
  | Float -> Wrapper.Column.read_float t ~column
  | Utf8 -> Wrapper.Column.read_utf8 t ~column
  | Date ->
    (try Wrapper.Column.read_date t ~column with
    | _ -> Wrapper.Column.read_utf8 t ~column |> Array.map ~f:Core_kernel.Date.of_string)
  | Time_ns ->
    (try Wrapper.Column.read_time_ns t ~column with
    | _ ->
      Wrapper.Column.read_utf8 t ~column |> Array.map ~f:Core_kernel.Time_ns.of_string)
  | Bool ->
    let bs = Wrapper.Column.read_bitset t ~column in
    Array.init (Valid.length bs) ~f:(Valid.get bs)

let read_opt (type a) t ~column (col_type : a col_type) : a option array =
  match col_type with
  | Int -> Wrapper.Column.read_int_opt t ~column
  | Float -> Wrapper.Column.read_float_opt t ~column
  | Utf8 -> Wrapper.Column.read_utf8_opt t ~column
  | Date ->
    (try Wrapper.Column.read_date_opt t ~column with
    | _ ->
      Wrapper.Column.read_utf8_opt t ~column
      |> Array.map ~f:(Option.map ~f:Core_kernel.Date.of_string))
  | Time_ns ->
    (try Wrapper.Column.read_time_ns_opt t ~column with
    | _ ->
      Wrapper.Column.read_utf8_opt t ~column
      |> Array.map ~f:(Option.map ~f:Core_kernel.Time_ns.of_string))
  | Bool ->
    let bs, valid = Wrapper.Column.read_bitset_opt t ~column in
    Array.init (Valid.length bs) ~f:(fun i ->
        if Valid.get valid i then Valid.get bs i |> Option.some else None)
