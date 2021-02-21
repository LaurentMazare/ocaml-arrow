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

  let col_multi ?name field ~f =
    let name = Option.value name ~default:(Field.name field) in
    fun rows -> f (Array.map rows ~f:(Field.get field)) ~name

  let col ?name field ~f =
    let name = Option.value name ~default:(Field.name field) in
    fun rows -> [ f (Array.map rows ~f:(Field.get field)) ~name ]

  let i64 = col ~f:Writer.int
  let i64_opt = col ~f:Writer.int_opt
  let f64 = col ~f:Writer.float
  let f64_opt = col ~f:Writer.float_opt
  let str = col ~f:Writer.utf8
  let str_opt = col ~f:Writer.utf8_opt
  let date = col ~f:Writer.date
  let date_opt = col ~f:Writer.date_opt
  let time_ns = col ~f:Writer.time_ns
  let time_ns_opt = col ~f:Writer.time_ns_opt

  let array_to_table cols rows =
    let cols = List.concat_map cols ~f:(fun col_fn -> col_fn rows) in
    Writer.create_table ~cols
end

module type Row_intf = sig
  type row

  val array_to_table : row array -> Table.t
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
