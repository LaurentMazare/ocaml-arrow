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

module F = struct
  type ('a, 'row, 'elem) col =
    ?name:string -> ('a, 'row, 'elem) Field.t_with_perm -> 'row array -> Writer.col list

  type ('a, 'row, 'elem) col_array =
    ?name:string
    -> suffixes:string array
    -> ('a, 'row, 'elem array) Field.t_with_perm
    -> 'row array
    -> Writer.col list

  let col_multi ?name field ~f =
    let name = Option.value name ~default:(Field.name field) in
    fun rows -> f (Array.map rows ~f:(Field.get field)) ~name

  let col ?name field ~f =
    let name = Option.value name ~default:(Field.name field) in
    fun rows -> [ f (Array.map rows ~f:(Field.get field)) ~name ]

  let c (type a) (col_type : a Table.col_type) =
    col ~f:(fun array -> Table.col array col_type)

  let c_opt (type a) (col_type : a Table.col_type) =
    col ~f:(fun array -> Table.col_opt array col_type)

  let col_array ~f ?name ~suffixes field =
    let name = Option.value name ~default:(Field.name field) in
    fun rows ->
      Array.mapi suffixes ~f:(fun col_index suffix ->
          let col =
            Array.map rows ~f:(fun row ->
                let field = Field.get field row in
                if Array.length field <> Array.length suffixes
                then failwith "unexpected size for %s, got %d, expected %d";
                field.(col_index))
          in
          f col ~name:(name ^ suffix))
      |> Array.to_list

  let c_array (type a) (col_type : a Table.col_type) =
    col_array ~f:(fun array -> Table.col array col_type)

  let c_opt_array (type a) (col_type : a Table.col_type) =
    col_array ~f:(fun array -> Table.col_opt array col_type)

  let array_to_table cols rows =
    let cols = List.concat_map cols ~f:(fun col_fn -> col_fn rows) in
    Writer.create_table ~cols

  let c_ignore = col_multi ~f:(fun _ ~name:_ -> [])

  let c_flatten array_to_table ?name:_ field array =
    let field_array = Array.map array ~f:(Field.get field) in
    List.concat_map array_to_table ~f:(fun fn -> fn field_array)
end

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

  let c ?name (type a) (col_type : a Table.col_type) field =
    let name = Option.value name ~default:(Field.name field) in
    [ P { name; get = Field.get field; col_type } ]

  let c_opt ?name (type a) (col_type : a Table.col_type) field =
    let name = Option.value name ~default:(Field.name field) in
    [ O { name; get = Field.get field; col_type } ]

  let c_ignore _field = []

  let c_flatten ?(rename = `prefix) field packed_cols =
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
