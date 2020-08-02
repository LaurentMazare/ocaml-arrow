open! Ctypes

(* https://arrow.apache.org/docs/format/CDataInterface.html *)
module C (F : Cstubs.FOREIGN) = struct
  open! F

  module Reader = struct
    type t = unit ptr

    let t : t typ = ptr void
    let read_file = foreign "read_file" (string @-> returning t)
    let close_file = foreign "close_file" (t @-> returning void)
    let num_rows_file = foreign "num_rows_file" (t @-> returning int64_t)
  end

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
    let get = foreign "get_schema" (Reader.t @-> returning (ptr t))
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

  let chunked_column =
    foreign
      "chunked_column"
      (Reader.t @-> int @-> ptr int @-> int @-> returning (ptr ArrowArray.t))

  let free_chunked_column =
    foreign "free_chunked_column" (ptr ArrowArray.t @-> int @-> returning void)

  let write_file =
    foreign
      "write_file"
      (string @-> ptr ArrowArray.t @-> ptr ArrowSchema.t @-> int @-> returning void)
end
