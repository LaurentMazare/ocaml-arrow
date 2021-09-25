open! Base

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

module Double = struct
  include Wrapper.DoubleBuilder

  let append_opt t v =
    match v with
    | None -> append_null t ~n:1
    | Some v -> append t v

  let length t = length t |> Int64.to_int_exn
  let null_count t = null_count t |> Int64.to_int_exn
end

module String = struct
  include Wrapper.StringBuilder

  let append_opt t v =
    match v with
    | None -> append_null t ~n:1
    | Some v -> append t v

  let length t = length t |> Int64.to_int_exn
  let null_count t = null_count t |> Int64.to_int_exn
end

module NativeInt = struct
  include Wrapper.Int64Builder

  let append t v = append t (Int64.of_int v)

  let append_opt t v =
    match v with
    | None -> append_null t ~n:1
    | Some v -> append t v

  let length t = length t |> Int64.to_int_exn
  let null_count t = null_count t |> Int64.to_int_exn
end

module Int32 = struct
  include Wrapper.Int32Builder

  let append_opt t v =
    match v with
    | None -> append_null t ~n:1
    | Some v -> append t v

  let length t = length t |> Int64.to_int_exn
  let null_count t = null_count t |> Int64.to_int_exn
end

module Int64 = struct
  include Wrapper.Int64Builder

  let append_opt t v =
    match v with
    | None -> append_null t ~n:1
    | Some v -> append t v

  let length t = length t |> Int64.to_int_exn
  let null_count t = null_count t |> Int64.to_int_exn
end

let make_table = Wrapper.Builder.make_table

module C = struct
  type ('row, 'elem, 'col_type) col =
    { name : string
    ; get : 'row -> 'elem
    ; col_type : 'col_type Table.col_type
    }

  type 'row packed_col =
    | P : ('row, 'elem, 'elem) col -> 'row packed_col
    | O : ('row, 'elem option, 'elem) col -> 'row packed_col

  type 'row packed_cols = 'row packed_col list

  let c (type a) (col_type : a Table.col_type) field =
    let name = Field.name field in
    [ P { name; get = Field.get field; col_type } ]

  let c_opt (type a) (col_type : a Table.col_type) field =
    let name = Field.name field in
    [ O { name; get = Field.get field; col_type } ]

  let c_map (type a) (col_type : a Table.col_type) field ~f =
    let name = Field.name field in
    let get row = Field.get field row |> f in
    [ P { name; get; col_type } ]

  let c_map_opt (type a) (col_type : a Table.col_type) field ~f =
    let name = Field.name field in
    let get row = Field.get field row |> f in
    [ O { name; get; col_type } ]

  let get ~suffixes field idx row =
    let n_elems = List.length suffixes in
    let row = Field.get field row in
    if Array.length row <> n_elems
    then
      Printf.failwithf
        "unexpected number of elements for %s: %d <> %d"
        (Field.name field)
        (Array.length row)
        n_elems
        ();
    row.(idx)

  let c_array (type a) (col_type : a Table.col_type) field ~suffixes =
    let name = Field.name field in
    List.mapi suffixes ~f:(fun idx suffix ->
        let get = get ~suffixes field idx in
        let name = name ^ suffix in
        P { name; get; col_type })

  let c_array_opt (type a) (col_type : a Table.col_type) field ~suffixes =
    let name = Field.name field in
    List.mapi suffixes ~f:(fun idx suffix ->
        let get = get ~suffixes field idx in
        let name = name ^ suffix in
        O { name; get; col_type })

  let c_ignore _field = []

  let c_flatten ?(rename = `prefix) packed_cols field =
    let rename =
      match rename with
      | `keep -> Fn.id
      | `prefix -> fun name -> Field.name field ^ "_" ^ name
      | `fn fn -> fn
    in
    List.map packed_cols ~f:(function
        | P { name; get; col_type } ->
          let name = rename name in
          let get row = Field.get field row |> get in
          P { name; get; col_type }
        | O { name; get; col_type } ->
          let name = rename name in
          let get row = Field.get field row |> get in
          O { name; get; col_type })

  let array_to_table packed_cols rows =
    let cols =
      List.map packed_cols ~f:(function
          | P { name; get; col_type } -> Table.col (Array.map rows ~f:get) col_type ~name
          | O { name; get; col_type } ->
            Table.col_opt (Array.map rows ~f:get) col_type ~name)
    in
    Writer.create_table ~cols
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

module Row (R : Row_intf) = struct
  type row = R.row

  type t =
    { mutable data : row list
    ; mutable length : int
    }

  let create () = { data = []; length = 0 }

  let append t row =
    t.data <- row :: t.data;
    t.length <- t.length + 1

  let to_table t = Array.of_list_rev t.data |> R.array_to_table
  let length t = t.length

  let reset t =
    t.data <- [];
    t.length <- 0
end
