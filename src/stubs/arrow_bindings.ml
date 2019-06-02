open Ctypes

module C (F : Cstubs.FOREIGN) = struct
  open F

  type gobject = unit ptr

  let gobject : gobject typ = ptr void
  let _array_builder = foreign "GARROW_ARRAY_BUILDER" (gobject @-> returning gobject)
  let object_unref = foreign "g_object_unref" (gobject @-> returning void)

  include Stubs_generated.C (F)
end
