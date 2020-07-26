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

  let const v _field reader_and_fields = (fun _idx -> v), reader_and_fields

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

  let write fold =
    let () = () in
    fun filename vs ->
      let length = List.length vs in
      let _length, cols, set = fold ~init:(length, [], fun _idx _t -> ()) in
      List.iteri vs ~f:set;
      let cols = List.map cols ~f:(fun col -> col ()) in
      Writer.write filename ~cols
end
