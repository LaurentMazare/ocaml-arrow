open Base

module Reader = struct
  type t = string list (* the column names *)

  type 'v col_ = t -> (Wrapper.Table.t * int -> 'v) * t
  type ('a, 'b, 'c, 'v) col = ('a, 'b, 'c) Field.t_with_perm -> 'v col_

  let with_memo ~get_col field_name t =
    let cache_get = ref None in
    let get (table, i) =
      let get =
        match !cache_get with
        | Some get -> get
        | None ->
          let get = get_col table ~column:(`Name field_name) in
          cache_get := Some get;
          get
      in
      get i
    in
    get, field_name :: t

  let i64 field =
    with_memo (Field.name field) ~get_col:(fun table ~column ->
        let ba = Wrapper.Column.read_i64_ba table ~column in
        fun i -> Int64.to_int_exn ba.{i})

  let date field =
    with_memo (Field.name field) ~get_col:(fun table ~column ->
        let a = Wrapper.Column.read_date table ~column in
        fun i -> a.(i))

  let time_ns field =
    with_memo (Field.name field) ~get_col:(fun table ~column ->
        let a = Wrapper.Column.read_time_ns table ~column in
        fun i -> a.(i))

  let f64 field =
    with_memo (Field.name field) ~get_col:(fun table ~column ->
        let ba = Wrapper.Column.read_f64_ba table ~column in
        fun i -> ba.{i})

  let bool field =
    with_memo (Field.name field) ~get_col:(fun table ~column ->
        let bs = Wrapper.Column.read_bitset table ~column in
        fun i -> Valid.get bs i)

  let str field =
    with_memo (Field.name field) ~get_col:(fun table ~column ->
        let a = Wrapper.Column.read_utf8 table ~column in
        fun i -> a.(i))

  let stringable (type a) (module S : Stringable.S with type t = a) field =
    with_memo (Field.name field) ~get_col:(fun table ~column ->
        let a = Wrapper.Column.read_utf8 table ~column in
        fun i -> a.(i) |> S.of_string)

  let i64_opt field =
    with_memo (Field.name field) ~get_col:(fun table ~column ->
        let ba, valid = Wrapper.Column.read_i64_ba_opt table ~column in
        fun i -> if Valid.get valid i then Some (Int64.to_int_exn ba.{i}) else None)

  let date_opt field =
    with_memo (Field.name field) ~get_col:(fun table ~column ->
        let a = Wrapper.Column.read_date_opt table ~column in
        fun i -> a.(i))

  let time_ns_opt field =
    with_memo (Field.name field) ~get_col:(fun table ~column ->
        let a = Wrapper.Column.read_time_ns_opt table ~column in
        fun i -> a.(i))

  let f64_opt field =
    with_memo (Field.name field) ~get_col:(fun table ~column ->
        let ba, valid = Wrapper.Column.read_f64_ba_opt table ~column in
        fun i -> if Valid.get valid i then Some ba.{i} else None)

  let str_opt field =
    with_memo (Field.name field) ~get_col:(fun table ~column ->
        let a = Wrapper.Column.read_utf8_opt table ~column in
        fun i -> a.(i))

  let bool_opt field =
    with_memo (Field.name field) ~get_col:(fun table ~column ->
        let bs, valid = Wrapper.Column.read_bitset_opt table ~column in
        fun i -> if Valid.get valid i then Some (Valid.get bs i) else None)

  let stringable_opt (type a) (module S : Stringable.S with type t = a) field =
    with_memo (Field.name field) ~get_col:(fun table ~column ->
        let array = Wrapper.Column.read_utf8_opt table ~column in
        fun i -> Option.map array.(i) ~f:S.of_string)

  let map col ~f field t =
    let get, t = col field t in
    (fun i -> get i |> f), t

  let read creator filename =
    let get_one, col_names = creator [] in
    let schema = File_reader.schema filename in
    let col_names = Set.of_list (module String) col_names in
    let column_idxs =
      List.filter_mapi schema.children ~f:(fun i schema ->
          let col_name = schema.Wrapper.Schema.name in
          if Set.mem col_names col_name then Some i else None)
    in
    let table = File_reader.table filename ~column_idxs in
    Wrapper.Table.num_rows table |> List.init ~f:(fun i -> get_one (table, i))
end

module Writer = struct
  module Writer = Wrapper.Writer

  type 'a state = int * (unit -> Wrapper.Writer.col) list * (int -> 'a -> unit)
  type ('a, 'b, 'c) col = 'a state -> ('b, 'a, 'c) Field.t_with_perm -> 'a state

  let i64 (length, acc_col, acc_set) field =
    let ba = Bigarray.Array1.create Int64 C_layout length in
    let col () = Writer.int64_ba ba ~name:(Field.name field) in
    let set idx t =
      ba.{idx} <- Field.get field t |> Int64.of_int;
      acc_set idx t
    in
    length, col :: acc_col, set

  let i64_opt (length, acc_col, acc_set) field =
    let ba = Bigarray.Array1.create Int64 C_layout length in
    let valid = Valid.create_all_valid length in
    let col () = Writer.int64_ba_opt ba valid ~name:(Field.name field) in
    let set idx t =
      (match Field.get field t with
      | Some v -> ba.{idx} <- Int64.of_int v
      | None -> Valid.set valid idx false);
      acc_set idx t
    in
    length, col :: acc_col, set

  let f64 (length, acc_col, acc_set) field =
    let ba = Bigarray.Array1.create Float64 C_layout length in
    let col () = Writer.float64_ba ba ~name:(Field.name field) in
    let set idx t =
      ba.{idx} <- Field.get field t;
      acc_set idx t
    in
    length, col :: acc_col, set

  let bool (length, acc_col, acc_set) field =
    let bs = Valid.create_all_valid length in
    let col () = Writer.bitset bs ~name:(Field.name field) in
    let set idx t =
      Valid.set bs idx (Field.get field t);
      acc_set idx t
    in
    length, col :: acc_col, set

  let bool_opt (length, acc_col, acc_set) field =
    let bs = Valid.create_all_valid length in
    let valid = Valid.create_all_valid length in
    let col () = Writer.bitset_opt bs ~valid ~name:(Field.name field) in
    let set idx t =
      (match Field.get field t with
      | Some v -> Valid.set bs idx v
      | None -> Valid.set valid idx false);
      acc_set idx t
    in
    length, col :: acc_col, set

  let f64_opt (length, acc_col, acc_set) field =
    let ba = Bigarray.Array1.create Float64 C_layout length in
    let valid = Valid.create_all_valid length in
    let col () = Writer.float64_ba_opt ba valid ~name:(Field.name field) in
    let set idx t =
      (match Field.get field t with
      | Some v -> ba.{idx} <- v
      | None -> Valid.set valid idx false);
      acc_set idx t
    in
    length, col :: acc_col, set

  let str (length, acc_col, acc_set) field =
    let strs = Array.create ~len:length "" in
    let col () = Writer.utf8 strs ~name:(Field.name field) in
    let set idx t =
      strs.(idx) <- Field.get field t;
      acc_set idx t
    in
    length, col :: acc_col, set

  let str_opt (length, acc_col, acc_set) field =
    let strs = Array.create ~len:length None in
    let col () = Writer.utf8_opt strs ~name:(Field.name field) in
    let set idx t =
      strs.(idx) <- Field.get field t;
      acc_set idx t
    in
    length, col :: acc_col, set

  let stringable
      (type a)
      (module S : Stringable.S with type t = a)
      (length, acc_col, acc_set)
      field
    =
    let strs = Array.create ~len:length "" in
    let col () = Writer.utf8 strs ~name:(Field.name field) in
    let set idx t =
      strs.(idx) <- Field.get field t |> S.to_string;
      acc_set idx t
    in
    length, col :: acc_col, set

  let date (length, acc_col, acc_set) field =
    let dates = Array.create ~len:length Core_kernel.Date.unix_epoch in
    let col () = Writer.date dates ~name:(Field.name field) in
    let set idx t =
      dates.(idx) <- Field.get field t;
      acc_set idx t
    in
    length, col :: acc_col, set

  let date_opt (length, acc_col, acc_set) field =
    let dates = Array.create ~len:length None in
    let col () = Writer.date_opt dates ~name:(Field.name field) in
    let set idx t =
      dates.(idx) <- Field.get field t;
      acc_set idx t
    in
    length, col :: acc_col, set

  let time_ns (length, acc_col, acc_set) field =
    let times = Array.create ~len:length Core_kernel.Time_ns.epoch in
    let col () = Writer.time_ns times ~name:(Field.name field) in
    let set idx t =
      times.(idx) <- Field.get field t;
      acc_set idx t
    in
    length, col :: acc_col, set

  let time_ns_opt (length, acc_col, acc_set) field =
    let times = Array.create ~len:length None in
    let col () = Writer.time_ns_opt times ~name:(Field.name field) in
    let set idx t =
      times.(idx) <- Field.get field t;
      acc_set idx t
    in
    length, col :: acc_col, set

  let write fold =
    let () = () in
    fun ?chunk_size ?compression filename vs ->
      let length = List.length vs in
      let _length, cols, set = fold ~init:(length, [], fun _idx _t -> ()) in
      List.iteri vs ~f:set;
      let cols = List.rev_map cols ~f:(fun col -> col ()) in
      Writer.write ?chunk_size ?compression filename ~cols
end

type 'a t =
  | Read of Reader.t
  | Write of 'a Writer.state

type ('a, 'b, 'c) col =
  ('a, 'b, 'c) Field.t_with_perm -> 'b t -> (Wrapper.Table.t * int -> 'c) * 'b t

let i64 field t =
  match t with
  | Read reader ->
    let get, reader = Reader.i64 field reader in
    get, Read reader
  | Write writer ->
    let writer = Writer.i64 writer field in
    (fun _ -> assert false), Write writer

let i64_opt field t =
  match t with
  | Read reader ->
    let get, reader = Reader.i64_opt field reader in
    get, Read reader
  | Write writer ->
    let writer = Writer.i64_opt writer field in
    (fun _ -> assert false), Write writer

let f64 field t =
  match t with
  | Read reader ->
    let get, reader = Reader.f64 field reader in
    get, Read reader
  | Write writer ->
    let writer = Writer.f64 writer field in
    (fun _ -> assert false), Write writer

let bool field t =
  match t with
  | Read reader ->
    let get, reader = Reader.bool field reader in
    get, Read reader
  | Write writer ->
    let writer = Writer.bool writer field in
    (fun _ -> assert false), Write writer

let f64_opt field t =
  match t with
  | Read reader ->
    let get, reader = Reader.f64_opt field reader in
    get, Read reader
  | Write writer ->
    let writer = Writer.f64_opt writer field in
    (fun _ -> assert false), Write writer

let str field t =
  match t with
  | Read reader ->
    let get, reader = Reader.str field reader in
    get, Read reader
  | Write writer ->
    let writer = Writer.str writer field in
    (fun _ -> assert false), Write writer

let str_opt field t =
  match t with
  | Read reader ->
    let get, reader = Reader.str_opt field reader in
    get, Read reader
  | Write writer ->
    let writer = Writer.str_opt writer field in
    (fun _ -> assert false), Write writer

let stringable (type a) (module S : Stringable.S with type t = a) field t =
  match t with
  | Read reader ->
    let get, reader = Reader.stringable (module S) field reader in
    get, Read reader
  | Write writer ->
    let writer = Writer.stringable (module S) writer field in
    (fun _ -> assert false), Write writer

let date field t =
  match t with
  | Read reader ->
    let get, reader = Reader.date field reader in
    get, Read reader
  | Write writer ->
    let writer = Writer.date writer field in
    (fun _ -> assert false), Write writer

let date_opt field t =
  match t with
  | Read reader ->
    let get, reader = Reader.date_opt field reader in
    get, Read reader
  | Write writer ->
    let writer = Writer.date_opt writer field in
    (fun _ -> assert false), Write writer

let time_ns field t =
  match t with
  | Read reader ->
    let get, reader = Reader.time_ns field reader in
    get, Read reader
  | Write writer ->
    let writer = Writer.time_ns writer field in
    (fun _ -> assert false), Write writer

let time_ns_opt field t =
  match t with
  | Read reader ->
    let get, reader = Reader.time_ns_opt field reader in
    get, Read reader
  | Write writer ->
    let writer = Writer.time_ns_opt writer field in
    (fun _ -> assert false), Write writer

let bool_opt field t =
  match t with
  | Read reader ->
    let get, reader = Reader.bool_opt field reader in
    get, Read reader
  | Write writer ->
    let writer = Writer.bool_opt writer field in
    (fun _ -> assert false), Write writer

let read_write_fn creator =
  let read filename =
    Reader.read
      (fun r ->
        match creator (Read r) with
        | get_one, Read col_names -> get_one, col_names
        | _, Write _ -> assert false)
      filename
  in
  let write ?chunk_size ?compression filename values =
    let length = List.length values in
    let _get_one, t = creator (Write (length, [], fun _ixd _t -> ())) in
    let cols, set =
      match t with
      | Read _ -> assert false
      | Write (_length, cols, set) -> cols, set
    in
    List.iteri values ~f:set;
    let cols = List.rev_map cols ~f:(fun col -> col ()) in
    Wrapper.Writer.write ?chunk_size ?compression filename ~cols
  in
  `read read, `write write
