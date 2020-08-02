open Base

module Reader = struct
  module Reader = Wrapper.Reader

  type t =
    { reader : Reader.t
    ; column_ids : (string, int, String.comparator_witness) Map.t
    ; num_rows : int option
    }

  let update_num_rows t ~num_rows =
    match t.num_rows with
    | None -> { t with num_rows = Some num_rows }
    | Some num_rows_ ->
      if num_rows_ <> num_rows
      then Printf.failwithf "column lengths mismatch %d <> %d" num_rows num_rows_ ();
      t

  type 'v col_ = t -> (int -> 'v) * t
  type ('a, 'b, 'c, 'v) col = ('a, 'b, 'c) Field.t_with_perm -> 'v col_

  let get_idx t field =
    match Map.find t.column_ids (Field.name field) with
    | Some idx -> idx
    | None -> Printf.failwithf "cannot find column %s" (Field.name field) ()

  let i64 field t =
    let column_idx = get_idx t field in
    let ba = Wrapper.Column.read_i64_ba t.reader ~column_idx in
    let get i = Int64.to_int_exn ba.{i} in
    get, update_num_rows t ~num_rows:(Bigarray.Array1.dim ba)

  let date field t =
    let column_idx = get_idx t field in
    let a = Wrapper.Column.read_date t.reader ~column_idx in
    Array.get a, update_num_rows t ~num_rows:(Array.length a)

  let time_ns field t =
    let column_idx = get_idx t field in
    let a = Wrapper.Column.read_time_ns t.reader ~column_idx in
    Array.get a, update_num_rows t ~num_rows:(Array.length a)

  let f64 field t =
    let column_idx = get_idx t field in
    let ba = Wrapper.Column.read_f64_ba t.reader ~column_idx in
    let get i = ba.{i} in
    get, update_num_rows t ~num_rows:(Bigarray.Array1.dim ba)

  let str field t =
    let column_idx = get_idx t field in
    let array = Wrapper.Column.read_utf8 t.reader ~column_idx in
    let get i = array.(i) in
    get, update_num_rows t ~num_rows:(Array.length array)

  let i64_opt field t =
    let column_idx = get_idx t field in
    let ba, valid = Wrapper.Column.read_i64_ba_opt t.reader ~column_idx in
    let get i = if Valid.get valid i then Some (Int64.to_int_exn ba.{i}) else None in
    get, update_num_rows t ~num_rows:(Bigarray.Array1.dim ba)

  let date_opt field t =
    let column_idx = get_idx t field in
    let a = Wrapper.Column.read_date_opt t.reader ~column_idx in
    Array.get a, update_num_rows t ~num_rows:(Array.length a)

  let time_ns_opt field t =
    let column_idx = get_idx t field in
    let a = Wrapper.Column.read_time_ns_opt t.reader ~column_idx in
    Array.get a, update_num_rows t ~num_rows:(Array.length a)

  let f64_opt field t =
    let column_idx = get_idx t field in
    let ba, valid = Wrapper.Column.read_f64_ba_opt t.reader ~column_idx in
    let get i = if Valid.get valid i then Some ba.{i} else None in
    get, update_num_rows t ~num_rows:(Bigarray.Array1.dim ba)

  let str_opt field t =
    let column_idx = get_idx t field in
    let array = Wrapper.Column.read_utf8_opt t.reader ~column_idx in
    let get i = array.(i) in
    get, update_num_rows t ~num_rows:(Array.length array)

  let read creator filename =
    Reader.with_file filename ~f:(fun reader ->
        let schema = Wrapper.Schema.get reader in
        let column_ids =
          schema.children
          |> List.mapi ~f:(fun i schema -> schema.Wrapper.Schema.name, i)
          |> Map.of_alist_exn (module String)
        in
        let get_one, t = creator { reader; column_ids; num_rows = None } in
        let num_rows =
          match t.num_rows with
          | None -> Printf.failwithf "no column in file %s" filename ()
          | Some num_rows -> num_rows
        in
        List.init num_rows ~f:get_one)
end

module Writer = struct
  module Writer = Wrapper.Writer

  type 'a state = int * (unit -> Wrapper.Writer.col) list * (int -> 'a -> unit)

  let i64 (length, acc_col, acc_set) field =
    let ba = Bigarray.Array1.create Int64 C_layout length in
    let col () = Writer.int64_ba ba ~name:(Field.name field) in
    let set idx t =
      ba.{idx} <- Field.get field t |> Int64.of_int;
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

  let str (length, acc_col, acc_set) field =
    let strs = Array.create ~len:length "" in
    let col () = Writer.utf8 strs ~name:(Field.name field) in
    let set idx t =
      strs.(idx) <- Field.get field t;
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

  let time_ns (length, acc_col, acc_set) field =
    let times = Array.create ~len:length Core_kernel.Time_ns.epoch in
    let col () = Writer.time_ns times ~name:(Field.name field) in
    let set idx t =
      times.(idx) <- Field.get field t;
      acc_set idx t
    in
    length, col :: acc_col, set

  let write fold =
    let () = () in
    fun filename vs ->
      let length = List.length vs in
      let _length, cols, set = fold ~init:(length, [], fun _idx _t -> ()) in
      List.iteri vs ~f:set;
      let cols = List.rev_map cols ~f:(fun col -> col ()) in
      Writer.write filename ~cols
end

type 'a t =
  | Read of Reader.t
  | Write of 'a Writer.state

type ('a, 'b, 'c) col = ('a, 'b, 'c) Field.t_with_perm -> 'b t -> (int -> 'c) * 'b t

let i64 field t =
  match t with
  | Read reader ->
    let get, reader = Reader.i64 field reader in
    get, Read reader
  | Write writer ->
    let writer = Writer.i64 writer field in
    (fun _ -> assert false), Write writer

let f64 field t =
  match t with
  | Read reader ->
    let get, reader = Reader.f64 field reader in
    get, Read reader
  | Write writer ->
    let writer = Writer.f64 writer field in
    (fun _ -> assert false), Write writer

let str field t =
  match t with
  | Read reader ->
    let get, reader = Reader.str field reader in
    get, Read reader
  | Write writer ->
    let writer = Writer.str writer field in
    (fun _ -> assert false), Write writer

let date field t =
  match t with
  | Read reader ->
    let get, reader = Reader.date field reader in
    get, Read reader
  | Write writer ->
    let writer = Writer.date writer field in
    (fun _ -> assert false), Write writer

let time_ns field t =
  match t with
  | Read reader ->
    let get, reader = Reader.time_ns field reader in
    get, Read reader
  | Write writer ->
    let writer = Writer.time_ns writer field in
    (fun _ -> assert false), Write writer

let read_write_fn creator =
  let read filename =
    Wrapper.Reader.with_file filename ~f:(fun reader ->
        let schema = Wrapper.Schema.get reader in
        let column_ids =
          schema.children
          |> List.mapi ~f:(fun i schema -> schema.Wrapper.Schema.name, i)
          |> Map.of_alist_exn (module String)
        in
        let reader = { Reader.reader; column_ids; num_rows = None } in
        let get_one, t = creator (Read reader) in
        let num_rows =
          match t with
          | Write _ -> assert false
          | Read reader ->
            (match reader.Reader.num_rows with
            | None -> Printf.failwithf "no column in file %s" filename ()
            | Some num_rows -> num_rows)
        in
        List.init num_rows ~f:get_one)
  in
  let write filename values =
    let length = List.length values in
    let _get_one, t = creator (Write (length, [], fun _ixd _t -> ())) in
    let cols, set =
      match t with
      | Read _ -> assert false
      | Write (_length, cols, set) -> cols, set
    in
    List.iteri values ~f:set;
    let cols = List.rev_map cols ~f:(fun col -> col ()) in
    Wrapper.Writer.write filename ~cols
  in
  `read read, `write write
