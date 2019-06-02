open Ctypes

module C (F : Cstubs.FOREIGN) = struct
  open F

  type t = unit ptr

  let t : t typ = ptr void
  let array_get_length = foreign "garrow_array_get_length" (t @-> returning int64_t)
  let array_builder = foreign "GARROW_ARRAY_BUILDER" (t @-> returning t)
  let null_array_new = foreign "garrow_null_array_new" (int64_t @-> returning t)
  let object_unref = foreign "g_object_unref" (t @-> returning void)
end
