open! Base
module C = C_api.C

let add_compact = false

external use_value : 'a -> unit = "ctypes_use" [@@noalloc]

let get_string ptr_char =
  let rec loop acc p =
    let c = Ctypes.(!@p) in
    if Char.( = ) c '\000'
    then List.rev acc |> String.of_char_list
    else loop (c :: acc) Ctypes.(p +@ 1)
  in
  loop [] ptr_char

module Schema = struct
  module Flags : sig
    type t [@@deriving sexp_of]

    val of_cint : Int64.t -> t
    val to_cint : t -> Int64.t
    val none : t
    val all : t list -> t
    val dictionary_ordered_ : t
    val nullable_ : t
    val map_keys_sorted_ : t
    val dictionary_ordered : t -> bool
    val nullable : t -> bool
    val map_keys_sorted : t -> bool
  end = struct
    type t = int

    let none = 0
    let all ts = List.reduce ts ~f:( lor ) |> Option.value ~default:0
    let dictionary_ordered_ = 1
    let nullable_ = 2
    let map_keys_sorted_ = 4
    let of_cint = Int64.to_int_exn
    let to_cint = Int64.of_int_exn
    let dictionary_ordered t = t land dictionary_ordered_ <> 0
    let nullable t = t land nullable_ <> 0
    let map_keys_sorted t = t land map_keys_sorted_ <> 0

    let sexp_of_t t =
      let maybe_add bool str acc = if bool then str :: acc else acc in
      []
      |> maybe_add (dictionary_ordered t) "dicitionary_ordered"
      |> maybe_add (nullable t) "nullable"
      |> maybe_add (map_keys_sorted t) "map_keys_sorted"
      |> [%sexp_of: string list]
  end

  type t =
    { format : Datatype.t
    ; name : string
    ; metadata : (string * string) list
    ; flags : Flags.t
    ; children : t list
    }
  [@@deriving sexp_of]

  let dereference_int32 ptr =
    if Ctypes.is_null ptr
    then failwith "got null ptr"
    else
      Ctypes.from_voidp Ctypes.int32_t (Ctypes.to_voidp ptr)
      |> Ctypes.( !@ )
      |> Int32.to_int_exn

  let metadata p =
    if Ctypes.is_null p
    then []
    else (
      let nfields = dereference_int32 p in
      let rec loop p acc = function
        | 0 -> List.rev acc
        | n ->
          let key_len = dereference_int32 p in
          let p = Ctypes.(p +@ 4) in
          let key = Ctypes.string_from_ptr p ~length:key_len in
          let p = Ctypes.(p +@ key_len) in
          let value_len = dereference_int32 p in
          let p = Ctypes.(p +@ 4) in
          let value = Ctypes.string_from_ptr p ~length:value_len in
          let p = Ctypes.(p +@ value_len) in
          loop p ((key, value) :: acc) (n - 1)
      in
      loop Ctypes.(p +@ 4) [] nfields)

  let of_c c_schema =
    Caml.Gc.finalise C.ArrowSchema.free c_schema;
    let rec loop c_schema =
      if Ctypes.is_null c_schema then failwith "Got a null schema";
      let schema = Ctypes.( !@ ) c_schema in
      let n_children = Ctypes.getf schema C.ArrowSchema.n_children |> Int64.to_int_exn in
      let children = Ctypes.getf schema C.ArrowSchema.children in
      let children = List.init n_children ~f:(fun i -> loop Ctypes.(!@(children +@ i))) in
      { format =
          Ctypes.getf schema C.ArrowSchema.format |> get_string |> Datatype.of_cstring
      ; name = Ctypes.getf schema C.ArrowSchema.name |> get_string
      ; metadata = Ctypes.getf schema C.ArrowSchema.metadata |> metadata
      ; flags = Ctypes.getf schema C.ArrowSchema.flags |> Flags.of_cint
      ; children
      }
    in
    loop c_schema
end

module Table = struct
  type t = C.Table.t

  let schema t = C.Table.schema t |> Schema.of_c
  let num_rows t = C.Table.num_rows t |> Int64.to_int_exn

  let with_free t =
    Caml.Gc.finalise C.Table.free t;
    t

  let slice t ~offset ~length =
    C.Table.slice t (Int64.of_int offset) (Int64.of_int length) |> with_free

  let read_csv filename = C.csv_read_table filename |> with_free
  let read_json filename = C.json_read_table filename |> with_free

  let write_parquet
      ?(chunk_size = 1024 * 1024)
      ?(compression = Compression.Snappy)
      t
      filename
    =
    C.Table.parquet_write filename t chunk_size (Compression.to_cint compression)

  let write_feather
      ?(chunk_size = 1024 * 1024)
      ?(compression = Compression.Snappy)
      t
      filename
    =
    C.Table.feather_write filename t chunk_size (Compression.to_cint compression)
end

module Parquet_reader = struct
  let schema_and_num_rows filename =
    let num_rows = Ctypes.CArray.make Ctypes.int64_t 1 in
    let schema =
      C.Parquet_reader.schema filename (Ctypes.CArray.start num_rows) |> Schema.of_c
    in
    let num_rows = Ctypes.CArray.get num_rows 0 |> Int64.to_int_exn in
    schema, num_rows

  let schema filename = schema_and_num_rows filename |> fst

  let table ?(only_first = -1) ?use_threads ?(column_idxs = []) filename =
    let use_threads =
      match use_threads with
      | None -> -1
      | Some false -> 0
      | Some true -> 1
    in
    let column_idxs = Ctypes.CArray.of_list Ctypes.int column_idxs in
    C.Parquet_reader.read_table
      filename
      (Ctypes.CArray.start column_idxs)
      (Ctypes.CArray.length column_idxs)
      use_threads
      (Int64.of_int only_first)
    |> Table.with_free
end

module Feather_reader = struct
  let schema filename = C.Feather_reader.schema filename |> Schema.of_c

  let table ?(column_idxs = []) filename =
    let column_idxs = Ctypes.CArray.of_list Ctypes.int column_idxs in
    C.Feather_reader.read_table
      filename
      (Ctypes.CArray.start column_idxs)
      (Ctypes.CArray.length column_idxs)
    |> Table.with_free
end

(* https://arrow.apache.org/docs/format/Columnar.html *)
module Column = struct
  type column =
    [ `Index of int
    | `Name of string
    ]

  module Chunk = struct
    type t =
      { offset : int
      ; buffers : unit Ctypes.ptr list
      ; length : int
      ; null_count : int
      }

    let create chunk ~fail_on_null ~fail_on_offset =
      let null_count = Ctypes.getf chunk C.ArrowArray.null_count |> Int64.to_int_exn in
      if fail_on_null && null_count <> 0
      then Printf.failwithf "expected no null item but got %d" null_count ();
      let offset = Ctypes.getf chunk C.ArrowArray.offset |> Int64.to_int_exn in
      if fail_on_offset && offset <> 0
      then
        Printf.failwithf
          "offsets are not supported for this column type, got %d"
          offset
          ();
      let n_buffers = Ctypes.getf chunk C.ArrowArray.n_buffers |> Int64.to_int_exn in
      let buffers = Ctypes.getf chunk C.ArrowArray.buffers in
      let buffers = List.init n_buffers ~f:(fun i -> Ctypes.(!@(buffers +@ i))) in
      let length = Ctypes.getf chunk C.ArrowArray.length |> Int64.to_int_exn in
      { offset; buffers; length; null_count }

    let primitive_data_ptr t ~ctype =
      match t.buffers with
      (* The first array is for the (optional) validity bitmap. *)
      | _bitmap :: data :: _ -> Ctypes.(from_voidp ctype data +@ t.offset)
      | buffers ->
        Printf.failwithf "expected 2 columns or more, got %d" (List.length buffers) ()
  end

  module Datatype = struct
    type t =
      | Int64
      | Float64
      | Utf8
      | Date32
      | Timestamp
      | Bool

    let to_int = function
      | Int64 -> 0
      | Float64 -> 1
      | Utf8 -> 2
      | Date32 -> 3
      | Timestamp -> 4
      | Bool -> 5
  end

  let with_column table dt ~column ~f =
    let n_chunks = Ctypes.CArray.make Ctypes.int 1 in
    let chunked_column =
      match column with
      | `Name column_name ->
        C.Table.chunked_column_by_name
          table
          column_name
          (Ctypes.CArray.start n_chunks)
          (Datatype.to_int dt)
      | `Index column_idx ->
        C.Table.chunked_column
          table
          column_idx
          (Ctypes.CArray.start n_chunks)
          (Datatype.to_int dt)
    in
    let n_chunks = Ctypes.CArray.get n_chunks 0 in
    Exn.protect
      ~f:(fun () ->
        let chunks =
          List.init n_chunks ~f:(fun chunk_idx ->
              Ctypes.(!@(chunked_column +@ chunk_idx)))
        in
        f chunks)
      ~finally:(fun () -> C.free_chunked_column chunked_column n_chunks)

  let num_rows chunks =
    List.fold chunks ~init:0 ~f:(fun acc chunk ->
        let length = Ctypes.getf chunk C.ArrowArray.length in
        acc + Int64.to_int_exn length)

  let of_chunks chunks ~kind ~ctype =
    let num_rows = num_rows chunks in
    let dst = Bigarray.Array1.create kind C_layout num_rows in
    let _num_rows =
      List.fold chunks ~init:0 ~f:(fun dst_offset chunk ->
          let chunk = Chunk.create chunk ~fail_on_null:true ~fail_on_offset:false in
          let ptr = Chunk.primitive_data_ptr chunk ~ctype in
          let dst = Bigarray.Array1.sub dst dst_offset chunk.length in
          let src = Ctypes.bigarray_of_ptr Ctypes.array1 chunk.length kind ptr in
          Bigarray.Array1.blit src dst;
          dst_offset + chunk.length)
    in
    dst

  let read_ba table ~datatype ~kind ~ctype ~column =
    with_column table datatype ~column ~f:(of_chunks ~kind ~ctype)

  let read_ba_opt table ~datatype ~kind ~ctype ~column =
    with_column table datatype ~column ~f:(fun chunks ->
        let num_rows = num_rows chunks in
        let dst = Bigarray.Array1.create kind C_layout num_rows in
        let valid = Valid.create_all_valid num_rows in
        let _num_rows =
          List.fold chunks ~init:0 ~f:(fun dst_offset chunk ->
              let chunk = Chunk.create chunk ~fail_on_null:false ~fail_on_offset:true in
              if chunk.null_count = chunk.length
              then
                for i = 0 to chunk.length - 1 do
                  Valid.set valid (dst_offset + i) false
                done
              else (
                let ptr = Chunk.primitive_data_ptr chunk ~ctype in
                let dst = Bigarray.Array1.sub dst dst_offset chunk.length in
                let src = Ctypes.bigarray_of_ptr Ctypes.array1 chunk.length kind ptr in
                Bigarray.Array1.blit src dst;
                if chunk.null_count <> 0
                then (
                  let valid_ptr =
                    match chunk.buffers with
                    | bitmap :: _ -> Ctypes.(from_voidp uint8_t bitmap)
                    | _ -> assert false
                  in
                  (* TODO: optimize with some shift operations. *)
                  for bidx = 0 to ((chunk.length + 7) / 8) - 1 do
                    let byte = Ctypes.(!@(valid_ptr +@ bidx) |> Unsigned.UInt8.to_int) in
                    if byte <> 255
                    then (
                      let max_idx = min 8 (chunk.length - (8 * bidx)) in
                      let valid_offset = dst_offset + (8 * bidx) in
                      for idx = 0 to max_idx - 1 do
                        let v = byte land (1 lsl idx) <> 0 in
                        Valid.set valid (valid_offset + idx) v
                      done)
                  done));
              dst_offset + chunk.length)
        in
        dst, valid)

  let read_bitset table ~column =
    with_column table Bool ~column ~f:(fun chunks ->
        let num_rows = num_rows chunks in
        let bitset = Valid.create_all_valid num_rows in
        let _num_rows =
          List.fold chunks ~init:0 ~f:(fun dst_offset chunk ->
              let chunk = Chunk.create chunk ~fail_on_null:true ~fail_on_offset:true in
              let ptr_ = Chunk.primitive_data_ptr chunk ~ctype:Ctypes.uint8_t in
              for bidx = 0 to ((chunk.length + 7) / 8) - 1 do
                let byte = Ctypes.(!@(ptr_ +@ bidx) |> Unsigned.UInt8.to_int) in
                let max_idx = min 8 (chunk.length - (8 * bidx)) in
                let valid_offset = dst_offset + (8 * bidx) in
                for idx = 0 to max_idx - 1 do
                  let v = byte land (1 lsl idx) <> 0 in
                  Valid.set bitset (valid_offset + idx) v
                done
              done;
              dst_offset + chunk.length)
        in
        bitset)

  let read_bitset_opt table ~column =
    with_column table Bool ~column ~f:(fun chunks ->
        let num_rows = num_rows chunks in
        let bitset = Valid.create_all_valid num_rows in
        let valid = Valid.create_all_valid num_rows in
        let _num_rows =
          List.fold chunks ~init:0 ~f:(fun dst_offset chunk ->
              let chunk = Chunk.create chunk ~fail_on_null:false ~fail_on_offset:true in
              if chunk.null_count = chunk.length
              then
                for i = 0 to chunk.length - 1 do
                  Valid.set valid (dst_offset + i) false
                done
              else (
                let ptr_ = Chunk.primitive_data_ptr chunk ~ctype:Ctypes.uint8_t in
                for bidx = 0 to ((chunk.length + 7) / 8) - 1 do
                  let byte = Ctypes.(!@(ptr_ +@ bidx) |> Unsigned.UInt8.to_int) in
                  let max_idx = min 8 (chunk.length - (8 * bidx)) in
                  let valid_offset = dst_offset + (8 * bidx) in
                  for idx = 0 to max_idx - 1 do
                    let v = byte land (1 lsl idx) <> 0 in
                    Valid.set bitset (valid_offset + idx) v
                  done
                done;
                if chunk.null_count <> 0
                then (
                  let valid_ptr =
                    match chunk.buffers with
                    | bitmap :: _ -> Ctypes.(from_voidp uint8_t bitmap)
                    | _ -> assert false
                  in
                  (* TODO: optimize with some shift operations. *)
                  for bidx = 0 to ((chunk.length + 7) / 8) - 1 do
                    let byte = Ctypes.(!@(valid_ptr +@ bidx) |> Unsigned.UInt8.to_int) in
                    if byte <> 255
                    then (
                      let max_idx = min 8 (chunk.length - (8 * bidx)) in
                      let valid_offset = dst_offset + (8 * bidx) in
                      for idx = 0 to max_idx - 1 do
                        let v = byte land (1 lsl idx) <> 0 in
                        Valid.set valid (valid_offset + idx) v
                      done)
                  done));
              dst_offset + chunk.length)
        in
        bitset, valid)

  let read_i64_ba = read_ba ~datatype:Int64 ~kind:Int64 ~ctype:Ctypes.int64_t
  let read_i64_ba_opt = read_ba_opt ~datatype:Int64 ~kind:Int64 ~ctype:Ctypes.int64_t

  let read_date table ~column =
    let dst = read_ba table ~datatype:Date32 ~kind:Int32 ~ctype:Ctypes.int32_t ~column in
    let num_rows = Bigarray.Array1.dim dst in
    Array.init num_rows ~f:(fun idx ->
        Core_kernel.Date.(add_days unix_epoch (Int32.to_int_exn dst.{idx})))

  let read_date_opt table ~column =
    let dst, valid =
      read_ba_opt table ~datatype:Date32 ~kind:Int32 ~ctype:Ctypes.int32_t ~column
    in
    let num_rows = Bigarray.Array1.dim dst in
    Array.init num_rows ~f:(fun idx ->
        if Valid.get valid idx
        then
          Core_kernel.Date.(add_days unix_epoch (Int32.to_int_exn dst.{idx}))
          |> Option.some
        else None)

  let read_time_ns table ~column =
    let dst =
      read_ba table ~datatype:Timestamp ~kind:Int64 ~ctype:Ctypes.int64_t ~column
    in
    let num_rows = Bigarray.Array1.dim dst in
    Array.init num_rows ~f:(fun idx ->
        Core_kernel.Time_ns.of_int_ns_since_epoch (Int64.to_int_exn dst.{idx}))

  let read_time_ns_opt table ~column =
    let dst, valid =
      read_ba_opt table ~datatype:Timestamp ~kind:Int64 ~ctype:Ctypes.int64_t ~column
    in
    let num_rows = Bigarray.Array1.dim dst in
    Array.init num_rows ~f:(fun idx ->
        if Valid.get valid idx
        then
          Core_kernel.Time_ns.of_int_ns_since_epoch (Int64.to_int_exn dst.{idx})
          |> Option.some
        else None)

  let read_f64_ba = read_ba ~datatype:Float64 ~kind:Float64 ~ctype:Ctypes.double
  let read_f64_ba_opt = read_ba_opt ~datatype:Float64 ~kind:Float64 ~ctype:Ctypes.double

  let read_utf8 table ~column =
    with_column table Utf8 ~column ~f:(fun chunks ->
        let num_rows = num_rows chunks in
        let dst = Array.create "" ~len:num_rows in
        let _num_rows =
          List.fold chunks ~init:0 ~f:(fun dst_offset chunk ->
              let chunk = Chunk.create chunk ~fail_on_null:true ~fail_on_offset:false in
              (* The first array is for the (optional) bitmap.
                 The second array contains the offsets, using int32 for the normal string
                 arrays (int64 for large strings).
                 The third array contains the data.
              *)
              let offsets = Chunk.primitive_data_ptr chunk ~ctype:Ctypes.int32_t in
              let data =
                match chunk.buffers with
                | [ _; _; data ] -> Ctypes.from_voidp Ctypes.char data
                | _ -> failwith "expected 3 columns for utf8"
              in
              for idx = 0 to chunk.length - 1 do
                let str_offset = Ctypes.(!@(offsets +@ idx)) |> Int32.to_int_exn in
                let next_str_offset =
                  Ctypes.(!@(offsets +@ (idx + 1))) |> Int32.to_int_exn
                in
                let str =
                  Ctypes.string_from_ptr
                    Ctypes.(data +@ str_offset)
                    ~length:(next_str_offset - str_offset)
                in
                dst.(dst_offset + idx) <- str
              done;
              dst_offset + chunk.length)
        in
        dst)

  let read_utf8_opt table ~column =
    with_column table Utf8 ~column ~f:(fun chunks ->
        let num_rows = num_rows chunks in
        let dst = Array.create None ~len:num_rows in
        let _num_rows =
          List.fold chunks ~init:0 ~f:(fun dst_offset chunk ->
              let chunk = Chunk.create chunk ~fail_on_null:false ~fail_on_offset:true in
              if chunk.null_count <> chunk.length
              then (
                (* The first array is for the (optional) valid bitmap.
                   The second array contains the offsets, using int32 for the normal string
                   arrays (int64 for large strings).
                   The third array contains the data.
                *)
                let offsets = Chunk.primitive_data_ptr chunk ~ctype:Ctypes.int32_t in
                let valid, data =
                  match chunk.buffers with
                  | [ valid; _; data ] ->
                    let valid =
                      if Ctypes.is_null valid
                      then None
                      else Some Ctypes.(from_voidp uint8_t valid)
                    in
                    valid, Ctypes.from_voidp Ctypes.char data
                  | _ -> failwith "expected 3 columns for utf8"
                in
                for idx = 0 to chunk.length - 1 do
                  let is_valid =
                    if chunk.null_count = 0
                    then true
                    else (
                      match valid with
                      | None -> true
                      | Some valid ->
                        let b =
                          Ctypes.(!@(valid +@ (idx / 8))) |> Unsigned.UInt8.to_int
                        in
                        b land (1 lsl (idx land 0b111)) <> 0)
                  in
                  if is_valid
                  then (
                    let str_offset = Ctypes.(!@(offsets +@ idx)) |> Int32.to_int_exn in
                    let next_str_offset =
                      Ctypes.(!@(offsets +@ (idx + 1))) |> Int32.to_int_exn
                    in
                    let str =
                      Ctypes.string_from_ptr
                        Ctypes.(data +@ str_offset)
                        ~length:(next_str_offset - str_offset)
                    in
                    dst.(dst_offset + idx) <- Some str)
                done);
              dst_offset + chunk.length)
        in
        dst)
end

module Writer = struct
  let verbose = false

  module Release_array_fn_ptr =
  (val Foreign.dynamic_funptr Ctypes.(C.ArrowArray.t @-> returning void))

  module Release_schema_fn_ptr =
  (val Foreign.dynamic_funptr Ctypes.(C.ArrowSchema.t @-> returning void))

  (* For now [release_schema] and [release_array] don't do anything as the
     memory is entirely managed on the ocaml side. *)
  let release_schema =
    let release_schema _ = if verbose then Stdio.printf "release schema\n%!" in
    Release_schema_fn_ptr.of_fun release_schema

  let release_array =
    let release_array _ = if verbose then Stdio.printf "release array\n%!" in
    Release_array_fn_ptr.of_fun release_array

  let release_array_ptr = Ctypes.(coerce Release_array_fn_ptr.t (ptr void) release_array)

  let release_schema_ptr =
    Ctypes.(coerce Release_schema_fn_ptr.t (ptr void) release_schema)

  let empty_schema_l = Ctypes.CArray.of_list (Ctypes.ptr C.ArrowSchema.t) []
  let empty_array_l = Ctypes.CArray.of_list (Ctypes.ptr C.ArrowArray.t) []
  let single_null_buffer = Ctypes.CArray.of_list (Ctypes.ptr Ctypes.void) [ Ctypes.null ]

  type col = C.ArrowArray.t * C.ArrowSchema.t

  let schema_struct ~format ~name ~children ~flag =
    let format = Ctypes.CArray.of_string format in
    let name = Ctypes.CArray.of_string name in
    let s =
      Ctypes.make C.ArrowSchema.t ~finalise:(fun _ ->
          use_value format;
          use_value name;
          use_value children)
    in
    Ctypes.setf s C.ArrowSchema.format (Ctypes.CArray.start format);
    Ctypes.setf s C.ArrowSchema.name (Ctypes.CArray.start name);
    Ctypes.setf s C.ArrowSchema.metadata (Ctypes.null |> Ctypes.from_voidp Ctypes.char);
    Ctypes.setf s C.ArrowSchema.flags Schema.Flags.(to_cint flag);
    Ctypes.setf s C.ArrowSchema.n_children (Ctypes.CArray.length children |> Int64.of_int);
    Ctypes.setf s C.ArrowSchema.children (Ctypes.CArray.start children);
    Ctypes.setf
      s
      C.ArrowSchema.dictionary
      (Ctypes.null |> Ctypes.from_voidp C.ArrowSchema.t);
    Ctypes.setf s C.ArrowSchema.release release_schema_ptr;
    s

  let array_struct ~null_count ~buffers ~children ~length ~finalise =
    let a =
      Ctypes.make
        ~finalise:(fun _ ->
          finalise ();
          use_value buffers;
          use_value children)
        C.ArrowArray.t
    in
    Ctypes.setf a C.ArrowArray.length (Int64.of_int length);
    Ctypes.setf a C.ArrowArray.null_count (Int64.of_int null_count);
    Ctypes.setf a C.ArrowArray.offset Int64.zero;
    Ctypes.setf a C.ArrowArray.n_buffers (Ctypes.CArray.length buffers |> Int64.of_int);
    Ctypes.setf a C.ArrowArray.buffers (Ctypes.CArray.start buffers);
    Ctypes.setf a C.ArrowArray.n_children (Ctypes.CArray.length children |> Int64.of_int);
    Ctypes.setf a C.ArrowArray.children (Ctypes.CArray.start children);
    Ctypes.setf a C.ArrowArray.dictionary (Ctypes.null |> Ctypes.from_voidp C.ArrowArray.t);
    Ctypes.setf a C.ArrowArray.release release_array_ptr;
    a

  let int64_ba array ~name =
    let buffers =
      Ctypes.CArray.of_list
        (Ctypes.ptr Ctypes.void)
        [ Ctypes.null; Ctypes.bigarray_start Array1 array |> Ctypes.to_voidp ]
    in
    let array_struct =
      array_struct
        ~buffers
        ~children:empty_array_l
        ~null_count:0
        ~finalise:(fun _ -> use_value array)
        ~length:(Bigarray.Array1.dim array)
    in
    let schema_struct =
      schema_struct ~format:"l" ~name ~children:empty_schema_l ~flag:Schema.Flags.none
    in
    (array_struct, schema_struct : col)

  let int64_ba_opt array valid ~name =
    if Bigarray.Array1.dim array <> Valid.length valid then failwith "incoherent lengths";
    let buffers =
      Ctypes.CArray.of_list
        (Ctypes.ptr Ctypes.void)
        [ Ctypes.bigarray_start Array1 (Valid.bigarray valid) |> Ctypes.to_voidp
        ; Ctypes.bigarray_start Array1 array |> Ctypes.to_voidp
        ]
    in
    let array_struct =
      array_struct
        ~buffers
        ~children:empty_array_l
        ~null_count:(Valid.num_false valid)
        ~finalise:(fun _ ->
          use_value array;
          use_value valid)
        ~length:(Bigarray.Array1.dim array)
    in
    let schema_struct =
      schema_struct
        ~format:"l"
        ~name
        ~children:empty_schema_l
        ~flag:Schema.Flags.nullable_
    in
    (array_struct, schema_struct : col)

  let date date_array ~name =
    let array = Ctypes.CArray.make Ctypes.int32_t (Array.length date_array) in
    Array.iteri date_array ~f:(fun idx date ->
        Ctypes.CArray.set
          array
          idx
          (Core_kernel.Date.(diff date unix_epoch) |> Int32.of_int_exn));
    let buffers =
      Ctypes.CArray.of_list
        (Ctypes.ptr Ctypes.void)
        [ Ctypes.null; Ctypes.CArray.start array |> Ctypes.to_voidp ]
    in
    let array_struct =
      array_struct
        ~buffers
        ~children:empty_array_l
        ~null_count:0
        ~finalise:(fun _ -> use_value array)
        ~length:(Ctypes.CArray.length array)
    in
    let schema_struct =
      schema_struct ~format:"tdD" ~name ~children:empty_schema_l ~flag:Schema.Flags.none
    in
    (array_struct, schema_struct : col)

  let date_opt date_array ~name =
    let array = Ctypes.CArray.make Ctypes.int32_t (Array.length date_array) in
    let valid = Valid.create_all_valid (Array.length date_array) in
    Array.iteri date_array ~f:(fun idx date ->
        let date =
          match date with
          | Some date -> Core_kernel.Date.(diff date unix_epoch) |> Int32.of_int_exn
          | None ->
            Valid.set valid idx false;
            Int32.zero
        in
        Ctypes.CArray.set array idx date);
    let buffers =
      Ctypes.CArray.of_list
        (Ctypes.ptr Ctypes.void)
        [ Ctypes.bigarray_start Array1 (Valid.bigarray valid) |> Ctypes.to_voidp
        ; Ctypes.CArray.start array |> Ctypes.to_voidp
        ]
    in
    let array_struct =
      array_struct
        ~buffers
        ~children:empty_array_l
        ~null_count:(Valid.num_false valid)
        ~finalise:(fun _ ->
          use_value array;
          use_value valid)
        ~length:(Ctypes.CArray.length array)
    in
    let schema_struct =
      schema_struct
        ~format:"tdD"
        ~name
        ~children:empty_schema_l
        ~flag:Schema.Flags.nullable_
    in
    (array_struct, schema_struct : col)

  let time_ns time_array ~name =
    let array = Ctypes.CArray.make Ctypes.int64_t (Array.length time_array) in
    Array.iteri time_array ~f:(fun idx time ->
        Ctypes.CArray.set
          array
          idx
          (Core_kernel.Time_ns.to_int_ns_since_epoch time |> Int64.of_int_exn));
    let buffers =
      Ctypes.CArray.of_list
        (Ctypes.ptr Ctypes.void)
        [ Ctypes.null; Ctypes.CArray.start array |> Ctypes.to_voidp ]
    in
    let array_struct =
      array_struct
        ~buffers
        ~children:empty_array_l
        ~null_count:0
        ~finalise:(fun _ -> use_value array)
        ~length:(Ctypes.CArray.length array)
    in
    let schema_struct =
      schema_struct
        ~format:"tsn:UTC"
        ~name
        ~children:empty_schema_l
        ~flag:Schema.Flags.none
    in
    (array_struct, schema_struct : col)

  let time_ns_opt time_array ~name =
    let array = Ctypes.CArray.make Ctypes.int64_t (Array.length time_array) in
    let valid = Valid.create_all_valid (Array.length time_array) in
    Array.iteri time_array ~f:(fun idx time ->
        let time =
          match time with
          | Some time ->
            Core_kernel.Time_ns.to_int_ns_since_epoch time |> Int64.of_int_exn
          | None ->
            Valid.set valid idx false;
            Int64.zero
        in
        Ctypes.CArray.set array idx time);
    let buffers =
      Ctypes.CArray.of_list
        (Ctypes.ptr Ctypes.void)
        [ Ctypes.bigarray_start Array1 (Valid.bigarray valid) |> Ctypes.to_voidp
        ; Ctypes.CArray.start array |> Ctypes.to_voidp
        ]
    in
    let array_struct =
      array_struct
        ~buffers
        ~children:empty_array_l
        ~null_count:(Valid.num_false valid)
        ~finalise:(fun _ ->
          use_value array;
          use_value valid)
        ~length:(Ctypes.CArray.length array)
    in
    let schema_struct =
      schema_struct
        ~format:"tsn:UTC"
        ~name
        ~children:empty_schema_l
        ~flag:Schema.Flags.nullable_
    in
    (array_struct, schema_struct : col)

  let float64_ba array ~name =
    let buffers =
      Ctypes.CArray.of_list
        (Ctypes.ptr Ctypes.void)
        [ Ctypes.null; Ctypes.bigarray_start Array1 array |> Ctypes.to_voidp ]
    in
    let array_struct =
      array_struct
        ~buffers
        ~children:empty_array_l
        ~null_count:0
        ~finalise:(fun _ -> use_value array)
        ~length:(Bigarray.Array1.dim array)
    in
    let schema_struct =
      schema_struct ~format:"g" ~name ~children:empty_schema_l ~flag:Schema.Flags.none
    in
    (array_struct, schema_struct : col)

  let bitset valid ~name =
    let array = Valid.bigarray valid in
    let buffers =
      Ctypes.CArray.of_list
        (Ctypes.ptr Ctypes.void)
        [ Ctypes.null; Ctypes.bigarray_start Array1 array |> Ctypes.to_voidp ]
    in
    let array_struct =
      array_struct
        ~buffers
        ~children:empty_array_l
        ~null_count:0
        ~finalise:(fun _ -> use_value array)
        ~length:(Valid.length valid)
    in
    let schema_struct =
      schema_struct ~format:"b" ~name ~children:empty_schema_l ~flag:Schema.Flags.none
    in
    (array_struct, schema_struct : col)

  let float64_ba_opt array valid ~name =
    if Bigarray.Array1.dim array <> Valid.length valid then failwith "incoherent lengths";
    let buffers =
      Ctypes.CArray.of_list
        (Ctypes.ptr Ctypes.void)
        [ Ctypes.bigarray_start Array1 (Valid.bigarray valid) |> Ctypes.to_voidp
        ; Ctypes.bigarray_start Array1 array |> Ctypes.to_voidp
        ]
    in
    let array_struct =
      array_struct
        ~buffers
        ~children:empty_array_l
        ~null_count:(Valid.num_false valid)
        ~finalise:(fun _ ->
          use_value array;
          use_value valid)
        ~length:(Bigarray.Array1.dim array)
    in
    let schema_struct =
      schema_struct
        ~format:"g"
        ~name
        ~children:empty_schema_l
        ~flag:Schema.Flags.nullable_
    in
    (array_struct, schema_struct : col)

  let bitset_opt content ~valid ~name =
    if Valid.length content <> Valid.length valid then failwith "incoherent lengths";
    let array = Valid.bigarray content in
    let buffers =
      Ctypes.CArray.of_list
        (Ctypes.ptr Ctypes.void)
        [ Ctypes.bigarray_start Array1 (Valid.bigarray valid) |> Ctypes.to_voidp
        ; Ctypes.bigarray_start Array1 array |> Ctypes.to_voidp
        ]
    in
    let array_struct =
      array_struct
        ~buffers
        ~children:empty_array_l
        ~null_count:(Valid.num_false valid)
        ~finalise:(fun _ ->
          use_value array;
          use_value valid)
        ~length:(Valid.length content)
    in
    let schema_struct =
      schema_struct
        ~format:"b"
        ~name
        ~children:empty_schema_l
        ~flag:Schema.Flags.nullable_
    in
    (array_struct, schema_struct : col)

  (* TODO: also have a "categorical" version? *)
  (* TODO: switch to large_utf8 if [sum_length >= Int32.max_value]. *)
  let utf8 array ~name =
    let length = Array.length array in
    let offsets = Bigarray.Array1.create Int32 C_layout (length + 1) in
    let sum_length =
      Array.foldi array ~init:0 ~f:(fun i acc str ->
          offsets.{i} <- Int32.of_int_exn acc;
          acc + String.length str)
    in
    offsets.{length} <- Int32.of_int_exn sum_length;
    let data = Ctypes.CArray.make Ctypes.char sum_length in
    Array.iteri array ~f:(fun i str ->
        let offset = offsets.{i} |> Int32.to_int_exn in
        for i = 0 to String.length str - 1 do
          Ctypes.CArray.set data (i + offset) str.[i]
        done);
    let buffers =
      Ctypes.CArray.of_list
        (Ctypes.ptr Ctypes.void)
        [ Ctypes.null
        ; Ctypes.bigarray_start Array1 offsets |> Ctypes.to_voidp
        ; Ctypes.CArray.start data |> Ctypes.to_voidp
        ]
    in
    let array_struct =
      array_struct
        ~buffers
        ~children:empty_array_l
        ~null_count:0
        ~finalise:(fun _ ->
          use_value data;
          use_value offsets)
        ~length
    in
    let schema_struct =
      schema_struct ~format:"u" ~name ~children:empty_schema_l ~flag:Schema.Flags.none
    in
    (array_struct, schema_struct : col)

  let utf8_opt array ~name =
    let valid = Valid.create_all_valid (Array.length array) in
    let length = Array.length array in
    let offsets = Bigarray.Array1.create Int32 C_layout (length + 1) in
    let sum_length =
      Array.foldi array ~init:0 ~f:(fun i acc str ->
          offsets.{i} <- Int32.of_int_exn acc;
          acc + Option.value_map str ~f:String.length ~default:0)
    in
    offsets.{length} <- Int32.of_int_exn sum_length;
    let data = Ctypes.CArray.make Ctypes.char sum_length in
    Array.iteri array ~f:(fun i str ->
        match str with
        | None -> Valid.set valid i false
        | Some str ->
          let offset = offsets.{i} |> Int32.to_int_exn in
          for i = 0 to String.length str - 1 do
            Ctypes.CArray.set data (i + offset) str.[i]
          done);
    let buffers =
      Ctypes.CArray.of_list
        (Ctypes.ptr Ctypes.void)
        [ Ctypes.bigarray_start Array1 (Valid.bigarray valid) |> Ctypes.to_voidp
        ; Ctypes.bigarray_start Array1 offsets |> Ctypes.to_voidp
        ; Ctypes.CArray.start data |> Ctypes.to_voidp
        ]
    in
    let array_struct =
      array_struct
        ~buffers
        ~children:empty_array_l
        ~null_count:(Valid.num_false valid)
        ~finalise:(fun _ ->
          use_value data;
          use_value offsets;
          use_value valid)
        ~length
    in
    let schema_struct =
      schema_struct
        ~format:"u"
        ~name
        ~children:empty_schema_l
        ~flag:Schema.Flags.nullable_
    in
    (array_struct, schema_struct : col)

  let write ?(chunk_size = 1024 * 1024) ?(compression = Compression.Snappy) filename ~cols
    =
    let children_arrays, children_schemas = List.unzip cols in
    let num_rows =
      match children_arrays with
      | [] -> 0
      | array :: _ -> Ctypes.getf array C.ArrowArray.length |> Int64.to_int_exn
    in
    let children_arrays =
      List.map children_arrays ~f:Ctypes.addr
      |> Ctypes.CArray.of_list (Ctypes.ptr C.ArrowArray.t)
    in
    let children_schemas =
      List.map children_schemas ~f:Ctypes.addr
      |> Ctypes.CArray.of_list (Ctypes.ptr C.ArrowSchema.t)
    in
    let array_struct =
      array_struct
        ~finalise:ignore
        ~length:num_rows
        ~null_count:0
        ~buffers:single_null_buffer
        ~children:children_arrays
    in
    let schema_struct =
      schema_struct
        ~format:"+s"
        ~name:""
        ~children:children_schemas
        ~flag:Schema.Flags.none
    in
    if add_compact then Caml.Gc.compact ();
    let write_fn =
      if String.is_suffix filename ~suffix:".feather"
      then C.feather_write_file
      else C.parquet_write_file
    in
    write_fn
      filename
      (Ctypes.addr array_struct)
      (Ctypes.addr schema_struct)
      chunk_size
      (Compression.to_cint compression);
    use_value cols

  let create_table ~cols =
    let children_arrays, children_schemas = List.unzip cols in
    let num_rows =
      match children_arrays with
      | [] -> 0
      | array :: _ -> Ctypes.getf array C.ArrowArray.length |> Int64.to_int_exn
    in
    let children_arrays =
      List.map children_arrays ~f:Ctypes.addr
      |> Ctypes.CArray.of_list (Ctypes.ptr C.ArrowArray.t)
    in
    let children_schemas =
      List.map children_schemas ~f:Ctypes.addr
      |> Ctypes.CArray.of_list (Ctypes.ptr C.ArrowSchema.t)
    in
    let array_struct =
      array_struct
        ~finalise:ignore
        ~length:num_rows
        ~null_count:0
        ~buffers:single_null_buffer
        ~children:children_arrays
    in
    let schema_struct =
      schema_struct
        ~format:"+s"
        ~name:""
        ~children:children_schemas
        ~flag:Schema.Flags.none
    in
    if add_compact then Caml.Gc.compact ();
    let table =
      C.Table.create (Ctypes.addr array_struct) (Ctypes.addr schema_struct)
      |> Table.with_free
    in
    use_value cols;
    table
end
