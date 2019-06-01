open Ctypes

module C (F : Cstubs.FOREIGN) = struct
  open F

  let array_get_length = foreign "garrow_array_get_length" (ptr void @-> returning void)
  let g_object_unref = foreign "g_object_unref" (ptr void @-> returning void)
end
