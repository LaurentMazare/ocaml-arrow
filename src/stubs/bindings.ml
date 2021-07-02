open! Ctypes

(* https://arrow.apache.org/docs/format/CDataInterface.html *)
module C (F : Cstubs.FOREIGN) = struct
  open! F

  module ArrowSchema = struct
    type t = [ `schema ] structure

    let t : t typ = structure "ArrowSchema"
    let format = field t "format" (ptr char)
    let name = field t "name" (ptr char)
    let metadata = field t "metadata" (ptr char)
    let flags = field t "flags" int64_t
    let n_children = field t "n_children" int64_t
    let children = field t "children" (ptr (ptr t))
    let dictionary = field t "dictionary" (ptr t)
    let release = field t "release" (ptr void)
    let private_data = field t "private_data" (ptr void)
    let () = seal t
    let free = foreign "free_schema" (ptr t @-> returning void)
  end

  module ArrowArray = struct
    type t = [ `array ] structure

    let t : t typ = structure "ArrowArray"
    let length = field t "length" int64_t
    let null_count = field t "null_count" int64_t
    let offset = field t "offset" int64_t
    let n_buffers = field t "n_buffers" int64_t
    let n_children = field t "n_children" int64_t
    let buffers = field t "buffers" (ptr (ptr void))
    let children = field t "children" (ptr (ptr t))
    let dictionary = field t "dictionary" (ptr t)
    let release = field t "release" (ptr void)
    let private_data = field t "private_data" (ptr void)
    let () = seal t
  end

  module Table = struct
    type t = unit ptr

    let t : t typ = ptr void
    let concatenate = foreign "table_concatenate" (ptr t @-> int @-> returning t)
    let slice = foreign "table_slice" (t @-> int64_t @-> int64_t @-> returning t)
    let num_rows = foreign "table_num_rows" (t @-> returning int64_t)
    let schema = foreign "table_schema" (t @-> returning (ptr ArrowSchema.t))
    let free = foreign "free_table" (t @-> returning void)
    let to_string = foreign "table_to_string" (t @-> returning string)

    let timestamp_unit_in_ns =
      foreign "timestamp_unit_in_ns" (t @-> string @-> int @-> returning int)

    let chunked_column =
      foreign
        "table_chunked_column"
        (t @-> int @-> ptr int @-> int @-> returning (ptr ArrowArray.t))

    let chunked_column_by_name =
      foreign
        "table_chunked_column_by_name"
        (t @-> string @-> ptr int @-> int @-> returning (ptr ArrowArray.t))

    let parquet_write =
      foreign "parquet_write_table" (string @-> t @-> int @-> int @-> returning void)

    let feather_write =
      foreign "feather_write_table" (string @-> t @-> int @-> int @-> returning void)

    let create =
      foreign "create_table" (ptr ArrowArray.t @-> ptr ArrowSchema.t @-> returning t)
  end

  module Parquet_reader = struct
    type t = unit ptr

    let t : t typ = ptr void

    let schema =
      foreign "parquet_schema" (string @-> ptr int64_t @-> returning (ptr ArrowSchema.t))

    let read_table =
      foreign
        "parquet_read_table"
        (string @-> ptr int @-> int @-> int @-> int64_t @-> returning Table.t)

    let open_ =
      foreign
        "parquet_reader_open"
        (string @-> ptr int @-> int @-> int @-> int @-> int @-> int @-> returning t)

    let next = foreign "parquet_reader_next" (t @-> returning Table.t)
    let close = foreign "parquet_reader_close" (t @-> returning void)
    let free = foreign "parquet_reader_free" (t @-> returning void)
  end

  module Arrow_reader = struct
    let schema = foreign "arrow_schema" (string @-> returning (ptr ArrowSchema.t))
  end

  module Feather_reader = struct
    let schema = foreign "feather_schema" (string @-> returning (ptr ArrowSchema.t))

    let read_table =
      foreign "feather_read_table" (string @-> ptr int @-> int @-> returning Table.t)
  end

  let csv_read_table = foreign "csv_read_table" (string @-> returning Table.t)
  let json_read_table = foreign "json_read_table" (string @-> returning Table.t)

  let free_chunked_column =
    foreign "free_chunked_column" (ptr ArrowArray.t @-> int @-> returning void)

  let arrow_write_file =
    foreign
      "arrow_write_file"
      (string @-> ptr ArrowArray.t @-> ptr ArrowSchema.t @-> int @-> returning void)

  let parquet_write_file =
    foreign
      "parquet_write_file"
      (string
      @-> ptr ArrowArray.t
      @-> ptr ArrowSchema.t
      @-> int
      @-> int
      @-> returning void)

  let feather_write_file =
    foreign
      "feather_write_file"
      (string
      @-> ptr ArrowArray.t
      @-> ptr ArrowSchema.t
      @-> int
      @-> int
      @-> returning void)

  module DoubleBuilder = struct
    type t = unit ptr

    let t : t typ = ptr void
    let create = foreign "create_double_builder" (void @-> returning t)
    let append = foreign "append_double_builder" (t @-> float @-> returning void)
    let append_null = foreign "append_null_double_builder" (t @-> int @-> returning void)
    let free = foreign "free_double_builder" (t @-> returning void)
    let length = foreign "length_double_builder" (t @-> returning int64_t)
    let null_count = foreign "null_count_double_builder" (t @-> returning int64_t)
  end

  module Int64Builder = struct
    type t = unit ptr

    let t : t typ = ptr void
    let create = foreign "create_int64_builder" (void @-> returning t)
    let append = foreign "append_int64_builder" (t @-> int64_t @-> returning void)
    let append_null = foreign "append_null_int64_builder" (t @-> int @-> returning void)
    let free = foreign "free_int64_builder" (t @-> returning void)
    let length = foreign "length_int64_builder" (t @-> returning int64_t)
    let null_count = foreign "null_count_int64_builder" (t @-> returning int64_t)
  end

  module StringBuilder = struct
    type t = unit ptr

    let t : t typ = ptr void
    let create = foreign "create_string_builder" (void @-> returning t)
    let append = foreign "append_string_builder" (t @-> string @-> returning void)
    let append_null = foreign "append_null_string_builder" (t @-> int @-> returning void)
    let free = foreign "free_string_builder" (t @-> returning void)
    let length = foreign "length_string_builder" (t @-> returning int64_t)
    let null_count = foreign "null_count_string_builder" (t @-> returning int64_t)
  end

  let make_table =
    foreign "make_table" (ptr (ptr void) @-> ptr (ptr char) @-> int @-> returning Table.t)
end
