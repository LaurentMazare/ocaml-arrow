open Ctypes

module C (F : Cstubs.FOREIGN) = struct
  open F

  type gobject = unit ptr

  let gobject : gobject typ = ptr void
  let gobject_type = foreign "G_OBJECT_TYPE" (gobject @-> returning ulong)
  let object_unref = foreign "g_object_unref" (gobject @-> returning void)
  let strdup = foreign "strdup" (ptr char @-> returning string)

  module GError = struct
    type t = [ `error ] ptr

    let t : [ `error ] structure typ = structure "_GError"
    let domain = field t "domain" uint32_t
    let code = field t "code" uint32_t
    let message = field t "message" (ptr char)
    let () = seal t
    let free = foreign "g_error_free" (ptr t @-> returning void)
  end
end
