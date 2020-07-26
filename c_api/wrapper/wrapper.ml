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

module Format = struct
  type t =
    | Null
    | Boolean
    | Int8
    | Uint8
    | Int16
    | Uint16
    | Int32
    | Uint32
    | Int64
    | Uint64
    | Float16
    | Float32
    | Float64
    | Binary
    | Large_binary
    | Utf8_string
    | Large_utf8_string
    | Decimal128 of
        { precision : int
        ; scale : int
        }
    | Fixed_width_binary of { bytes : int }
    | Date32 of [ `days ]
    | Date64 of [ `milliseconds ]
    | Time32 of [ `seconds | `milliseconds ]
    | Time64 of [ `microseconds | `nanoseconds ]
    | Timestamp of
        { precision : [ `seconds | `milliseconds | `microseconds | `nanoseconds ]
        ; timezone : string
        }
    | Duration of [ `seconds | `milliseconds | `microseconds | `nanoseconds ]
    | Interval of [ `months | `days_time ]
    | Struct
    | Map
    | Unknown of string
  [@@deriving sexp]

  let of_string = function
    | "n" -> Null
    | "b" -> Boolean
    | "c" -> Int8
    | "C" -> Uint8
    | "s" -> Int16
    | "S" -> Uint16
    | "i" -> Int32
    | "I" -> Uint32
    | "l" -> Int64
    | "L" -> Uint64
    | "e" -> Float16
    | "f" -> Float32
    | "g" -> Float64
    | "z" -> Binary
    | "Z" -> Large_binary
    | "u" -> Utf8_string
    | "U" -> Large_utf8_string
    | "tdD" -> Date32 `days
    | "tdm" -> Date64 `milliseconds
    | "tts" -> Time32 `seconds
    | "ttm" -> Time32 `milliseconds
    | "ttu" -> Time64 `microseconds
    | "ttn" -> Time64 `nanoseconds
    | "tDs" -> Duration `seconds
    | "tDm" -> Duration `milliseconds
    | "tDu" -> Duration `microseconds
    | "tDn" -> Duration `nanoseconds
    | "tiM" -> Interval `months
    | "tiD" -> Interval `days_time
    | "+s" -> Struct
    | "+m" -> Map
    | unknown -> Unknown unknown
end

module Reader = struct
  type t = C.Reader.t

  let read = C.Reader.read_file
  let close = C.Reader.close_file

  let with_file filename ~f =
    let t = read filename in
    Exn.protect ~f:(fun () -> f t) ~finally:(fun () -> close t)
end

module Schema = struct
  type t =
    { format : Format.t
    ; name : string
    ; metadata : (string * string) list
    ; flags : int
    ; children : t list
    }
  [@@deriving sexp]

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

  let get reader =
    let c_schema = C.ArrowSchema.get reader in
    Caml.Gc.finalise C.ArrowSchema.free c_schema;
    let rec loop c_schema =
      if Ctypes.is_null c_schema then failwith "Got a null schema";
      let schema = Ctypes.( !@ ) c_schema in
      let n_children = Ctypes.getf schema C.ArrowSchema.n_children |> Int64.to_int_exn in
      let children = Ctypes.getf schema C.ArrowSchema.children in
      let children = List.init n_children ~f:(fun i -> loop Ctypes.(!@(children +@ i))) in
      { format = Ctypes.getf schema C.ArrowSchema.format |> get_string |> Format.of_string
      ; name = Ctypes.getf schema C.ArrowSchema.name |> get_string
      ; metadata = Ctypes.getf schema C.ArrowSchema.metadata |> metadata
      ; flags = Ctypes.getf schema C.ArrowSchema.flags |> Int64.to_int_exn
      ; children
      }
    in
    loop c_schema
end

(* https://arrow.apache.org/docs/format/Columnar.html *)
module Column = struct
  module Chunk = struct
    type t =
      { offset : int
      ; buffers : unit Ctypes.ptr list
      ; length : int
      }

    let create_no_null chunk =
      let null_count = Ctypes.getf chunk C.ArrowArray.null_count |> Int64.to_int_exn in
      if null_count <> 0
      then Printf.failwithf "expected no null item but got %d" null_count ();
      let offset = Ctypes.getf chunk C.ArrowArray.offset |> Int64.to_int_exn in
      let n_buffers = Ctypes.getf chunk C.ArrowArray.n_buffers |> Int64.to_int_exn in
      let buffers = Ctypes.getf chunk C.ArrowArray.buffers in
      let buffers = List.init n_buffers ~f:(fun i -> Ctypes.(!@(buffers +@ i))) in
      let length = Ctypes.getf chunk C.ArrowArray.length |> Int64.to_int_exn in
      { offset; buffers; length }

    let primitive_data_ptr t ~ctype ~name =
      match t.buffers with
      (* The first array is for the (optional) bitmap. *)
      | _bitmap :: data :: _ -> Ctypes.(from_voidp ctype data +@ t.offset)
      | buffers ->
        Printf.failwithf
          "expected 2 columns or more for %s column, got %d"
          name
          (List.length buffers)
          ()
  end

  module Datatype = struct
    type t =
      | Int64
      | Float64
      | Utf8

    let to_int = function
      | Int64 -> 0
      | Float64 -> 1
      | Utf8 -> 2
  end

  let with_column reader dt ~column_idx ~f =
    let n_chunks = Ctypes.CArray.make Ctypes.int 1 in
    let chunked_column =
      C.chunked_column
        reader
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

  let read_i64_ba reader ~column_idx =
    with_column reader Int64 ~column_idx ~f:(fun chunks ->
        let num_rows = num_rows chunks in
        let dst = Bigarray.Array1.create Int64 C_layout num_rows in
        let _num_rows =
          List.fold chunks ~init:0 ~f:(fun dst_offset chunk ->
              let chunk = Chunk.create_no_null chunk in
              let ptr =
                Chunk.primitive_data_ptr chunk ~ctype:Ctypes.int64_t ~name:"int64"
              in
              let dst = Bigarray.Array1.sub dst dst_offset chunk.length in
              let src = Ctypes.bigarray_of_ptr Ctypes.array1 chunk.length Int64 ptr in
              Bigarray.Array1.blit src dst;
              dst_offset + chunk.length)
        in
        dst)

  let read_f64_ba reader ~column_idx =
    with_column reader Float64 ~column_idx ~f:(fun chunks ->
        let num_rows = num_rows chunks in
        let dst = Bigarray.Array1.create Float64 C_layout num_rows in
        let _num_rows =
          List.fold chunks ~init:0 ~f:(fun dst_offset chunk ->
              let chunk = Chunk.create_no_null chunk in
              let ptr =
                Chunk.primitive_data_ptr chunk ~ctype:Ctypes.double ~name:"double"
              in
              let dst = Bigarray.Array1.sub dst dst_offset chunk.length in
              let src = Ctypes.bigarray_of_ptr Ctypes.array1 chunk.length Float64 ptr in
              Bigarray.Array1.blit src dst;
              dst_offset + chunk.length)
        in
        dst)

  let read_utf8 reader ~column_idx =
    with_column reader Utf8 ~column_idx ~f:(fun chunks ->
        let num_rows = num_rows chunks in
        let dst = Array.create "" ~len:num_rows in
        let _num_rows =
          List.fold chunks ~init:0 ~f:(fun dst_offset chunk ->
              let chunk = Chunk.create_no_null chunk in
              (* The first array is for the (optional) bitmap.
                 The second array contains the offsets, using int32 for the normal string
                 arrays (int64 for large strings).
                 The third array contains the data.
              *)
              let offsets =
                Chunk.primitive_data_ptr chunk ~ctype:Ctypes.int32_t ~name:"int32"
              in
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

  let empty_children = Ctypes.CArray.of_list (Ctypes.ptr C.ArrowSchema.t) []
  let single_null_buffer = Ctypes.CArray.of_list (Ctypes.ptr Ctypes.void) [ Ctypes.null ]

  type col = C.ArrowArray.t * C.ArrowSchema.t

  let int64_ba array ~name =
    let format = Ctypes.CArray.of_string "l" in
    let name = Ctypes.CArray.of_string name in
    let buffers =
      Ctypes.CArray.of_list
        (Ctypes.ptr Ctypes.void)
        [ Ctypes.null; Ctypes.bigarray_start Array1 array |> Ctypes.to_voidp ]
    in
    let array_struct =
      let a =
        Ctypes.make
          ~finalise:(fun _ ->
            use_value format;
            use_value name;
            use_value buffers;
            use_value array)
          C.ArrowArray.t
      in
      let length = Bigarray.Array1.dim array in
      Ctypes.setf a C.ArrowArray.length (Int64.of_int length);
      Ctypes.setf a C.ArrowArray.null_count Int64.zero;
      Ctypes.setf a C.ArrowArray.offset Int64.zero;
      Ctypes.setf a C.ArrowArray.n_buffers (Int64.of_int 2);
      Ctypes.setf a C.ArrowArray.buffers (Ctypes.CArray.start buffers);
      Ctypes.setf a C.ArrowArray.n_children Int64.zero;
      Ctypes.setf
        a
        C.ArrowArray.dictionary
        (Ctypes.null |> Ctypes.from_voidp C.ArrowArray.t);
      Ctypes.setf a C.ArrowArray.release release_array_ptr;
      a
    in
    let schema_struct =
      let s = Ctypes.make C.ArrowSchema.t in
      Ctypes.setf s C.ArrowSchema.format (Ctypes.CArray.start format);
      Ctypes.setf s C.ArrowSchema.name (Ctypes.CArray.start name);
      Ctypes.setf s C.ArrowSchema.metadata (Ctypes.null |> Ctypes.from_voidp Ctypes.char);
      Ctypes.setf s C.ArrowSchema.flags Int64.zero;
      Ctypes.setf s C.ArrowSchema.n_children Int64.zero;
      Ctypes.setf s C.ArrowSchema.children (Ctypes.CArray.start empty_children);
      Ctypes.setf
        s
        C.ArrowSchema.dictionary
        (Ctypes.null |> Ctypes.from_voidp C.ArrowSchema.t);
      Ctypes.setf s C.ArrowSchema.release release_schema_ptr;
      s
    in
    (array_struct, schema_struct : col)

  let float64_ba array ~name =
    let format = Ctypes.CArray.of_string "g" in
    let name = Ctypes.CArray.of_string name in
    let buffers =
      Ctypes.CArray.of_list
        (Ctypes.ptr Ctypes.void)
        [ Ctypes.null; Ctypes.bigarray_start Array1 array |> Ctypes.to_voidp ]
    in
    let array_struct =
      let a =
        Ctypes.make
          ~finalise:(fun _ ->
            use_value format;
            use_value name;
            use_value buffers;
            use_value array)
          C.ArrowArray.t
      in
      let length = Bigarray.Array1.dim array in
      Ctypes.setf a C.ArrowArray.length (Int64.of_int length);
      Ctypes.setf a C.ArrowArray.null_count Int64.zero;
      Ctypes.setf a C.ArrowArray.offset Int64.zero;
      Ctypes.setf a C.ArrowArray.n_buffers (Int64.of_int 2);
      Ctypes.setf a C.ArrowArray.buffers (Ctypes.CArray.start buffers);
      Ctypes.setf a C.ArrowArray.n_children Int64.zero;
      Ctypes.setf
        a
        C.ArrowArray.dictionary
        (Ctypes.null |> Ctypes.from_voidp C.ArrowArray.t);
      Ctypes.setf a C.ArrowArray.release release_array_ptr;
      a
    in
    let schema_struct =
      let s = Ctypes.make C.ArrowSchema.t in
      Ctypes.setf s C.ArrowSchema.format (Ctypes.CArray.start format);
      Ctypes.setf s C.ArrowSchema.name (Ctypes.CArray.start name);
      Ctypes.setf s C.ArrowSchema.metadata (Ctypes.null |> Ctypes.from_voidp Ctypes.char);
      Ctypes.setf s C.ArrowSchema.flags Int64.zero;
      Ctypes.setf s C.ArrowSchema.n_children Int64.zero;
      Ctypes.setf s C.ArrowSchema.children (Ctypes.CArray.start empty_children);
      Ctypes.setf
        s
        C.ArrowSchema.dictionary
        (Ctypes.null |> Ctypes.from_voidp C.ArrowSchema.t);
      Ctypes.setf s C.ArrowSchema.release release_schema_ptr;
      s
    in
    (array_struct, schema_struct : col)

  (* TODO: also have a "categorical" version? *)
  let utf8 array ~name =
    let format = Ctypes.CArray.of_string "u" in
    let name = Ctypes.CArray.of_string name in
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
      let a =
        Ctypes.make
          ~finalise:(fun _ ->
            use_value format;
            use_value name;
            use_value buffers;
            use_value data;
            use_value offsets)
          C.ArrowArray.t
      in
      Ctypes.setf a C.ArrowArray.length (Int64.of_int length);
      Ctypes.setf a C.ArrowArray.null_count Int64.zero;
      Ctypes.setf a C.ArrowArray.offset Int64.zero;
      Ctypes.setf a C.ArrowArray.n_buffers (Int64.of_int 3);
      Ctypes.setf a C.ArrowArray.buffers (Ctypes.CArray.start buffers);
      Ctypes.setf a C.ArrowArray.n_children Int64.zero;
      Ctypes.setf
        a
        C.ArrowArray.dictionary
        (Ctypes.null |> Ctypes.from_voidp C.ArrowArray.t);
      Ctypes.setf a C.ArrowArray.release release_array_ptr;
      a
    in
    let schema_struct =
      let s = Ctypes.make C.ArrowSchema.t in
      Ctypes.setf s C.ArrowSchema.format (Ctypes.CArray.start format);
      Ctypes.setf s C.ArrowSchema.name (Ctypes.CArray.start name);
      Ctypes.setf s C.ArrowSchema.metadata (Ctypes.null |> Ctypes.from_voidp Ctypes.char);
      Ctypes.setf s C.ArrowSchema.flags Int64.zero;
      Ctypes.setf s C.ArrowSchema.n_children Int64.zero;
      Ctypes.setf s C.ArrowSchema.children (Ctypes.CArray.start empty_children);
      Ctypes.setf
        s
        C.ArrowSchema.dictionary
        (Ctypes.null |> Ctypes.from_voidp C.ArrowSchema.t);
      Ctypes.setf s C.ArrowSchema.release release_schema_ptr;
      s
    in
    (array_struct, schema_struct : col)

  let write ?(chunk_size = 1024 * 1024) filename ~cols =
    let format = Ctypes.CArray.of_string "+s" in
    let name = Ctypes.CArray.of_string "" in
    let n_children = List.length cols |> Int64.of_int in
    let children_arrays, children_schemas = List.unzip cols in
    let num_rows =
      match children_arrays with
      | [] -> Int64.zero
      | array :: _ -> Ctypes.getf array C.ArrowArray.length
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
      let a = Ctypes.make C.ArrowArray.t in
      Ctypes.setf a C.ArrowArray.length num_rows;
      Ctypes.setf a C.ArrowArray.null_count Int64.zero;
      Ctypes.setf a C.ArrowArray.offset Int64.zero;
      Ctypes.setf a C.ArrowArray.n_buffers Int64.one;
      Ctypes.setf a C.ArrowArray.buffers (Ctypes.CArray.start single_null_buffer);
      Ctypes.setf a C.ArrowArray.n_children n_children;
      Ctypes.setf a C.ArrowArray.children (Ctypes.CArray.start children_arrays);
      Ctypes.setf
        a
        C.ArrowArray.dictionary
        (Ctypes.null |> Ctypes.from_voidp C.ArrowArray.t);
      Ctypes.setf a C.ArrowArray.release release_array_ptr;
      a
    in
    let schema_struct =
      let s = Ctypes.make C.ArrowSchema.t in
      Ctypes.setf s C.ArrowSchema.format (Ctypes.CArray.start format);
      Ctypes.setf s C.ArrowSchema.name (Ctypes.CArray.start name);
      Ctypes.setf s C.ArrowSchema.metadata (Ctypes.null |> Ctypes.from_voidp Ctypes.char);
      Ctypes.setf s C.ArrowSchema.flags Int64.zero;
      Ctypes.setf s C.ArrowSchema.n_children n_children;
      Ctypes.setf s C.ArrowSchema.children (Ctypes.CArray.start children_schemas);
      Ctypes.setf
        s
        C.ArrowSchema.dictionary
        (Ctypes.null |> Ctypes.from_voidp C.ArrowSchema.t);
      Ctypes.setf s C.ArrowSchema.release release_schema_ptr;
      s
    in
    if add_compact then Caml.Gc.compact ();
    C.write_file
      filename
      (Ctypes.addr array_struct)
      (Ctypes.addr schema_struct)
      chunk_size;
    use_value cols;
    use_value children_arrays;
    use_value children_schemas;
    use_value name;
    use_value format
end
