# The code generated relies on inheritance working without
# applying any macros just by casting the pointer.
#
# TODO: check that it is the case.
# TODO: list/array support, e.g. for [get_values] and [append_values].
import os
import re
os.environ['GI_TYPELIB_PATH'] = 'gen'

verbose = True

snake_exceptions = {
    'array': 'array_',
    'new': 'new_',
}

c_ml_types = {
    'gint64': { 'c': 'int64_t', 'ml': 'Int64.t', 'base': True },
    'gint32': { 'c': 'int32_t', 'ml': 'Int32.t', 'base': True },
    'gint16': { 'c': 'int16_t', 'ml': 'int', 'base': True },
    'gint8': { 'c': 'int8_t', 'ml': 'int', 'base': True },
    'guint64': { 'c': 'uint64_t', 'ml': 'Unsigned.uint64', 'base': True },
    'guint32': { 'c': 'uint32_t', 'ml': 'Unsigned.uint32', 'base': True },
    'guint16': { 'c': 'uint16_t', 'ml': 'Unsigned.uint16', 'base': True },
    'guint8': { 'c': 'uint8_t', 'ml': 'Unsigned.uint8', 'base': True },
    'gdouble': { 'c': 'double', 'ml': 'float', 'base': True },
    'gfloat': { 'c': 'float', 'ml': 'float', 'base': True },
    'gboolean': { 'c': 'bool', 'ml': 'bool', 'base': True },
    'utf8': { 'c': 'string', 'ml': 'string', 'base': True },
}

unsupported_types = set([
  'bytes', 'type', 'compression_type', 'metadata_version', 'time_unit' ])

def snake_case(name):
  s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
  res = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
  return snake_exceptions.get(res, res)

import gi
gi.require_version('Arrow', '1.0')
from gi.repository import Arrow
rep = gi.Repository.get_default()

def variant_type(p):
  onames = []
  while True:
    pname = p.get_name()
    if pname == 'Object': break
    onames.append('`%s' % snake_case(pname))
    p = p.get_parent()
    if p is None or p.get_name() == pname: break

  return '[ %s ] gobject' % ' | '.join(onames)

def c_ml_type(type_, is_arg=None):
  t = type_.get_tag_as_string()
  ct = c_ml_types.get(t, None)
  if ct is not None: return ct
  if t == 'interface':
    oname = type_.get_interface().get_name()
    t = snake_case(oname)
    if t in unsupported_types: return None
    assert (is_arg is not None)
    if is_arg:
      ml = '[> `%s ] gobject' % t
    else:
      ml = variant_type(type_.get_interface())
    return { 'c': t, 'ml': ml, 'base': False }

def handle_object_info(oinfo, f_c, f_ml, f_mli):
  oname = oinfo.get_name()
  # ctype: oinfo.get_type_name()
  f_c.write('  module %s = struct\n' % oname)
  f_c.write('    type t = %s\n' % snake_case(oname))
  f_c.write('    let t : t typ = %s\n\n' % snake_case(oname))

  variant_t = variant_type(oinfo)
  f_ml.write('module %s = struct\n' % oname)
  f_ml.write('  type t = %s\n' % variant_t)
  f_mli.write('module %s : sig\n' % oname)
  f_mli.write('  type t = %s\n\n' % variant_t)

  get_type = oinfo.get_type_init()
  f_c.write('    let get_type = foreign "%s"\n' % get_type)
  f_c.write('      (void @-> returning ulong)\n\n')

  f_ml.write('  let of_gobject g =\n')
  f_ml.write('    if C.%s.get_type () = C.gobject_type g\n' % oname)
  f_ml.write('    then Some g else None\n\n')
  f_mli.write('  val of_gobject : _ gobject -> t option\n\n')

  for m in oinfo.get_methods():
    if m.is_deprecated(): continue
    try:
      mname = snake_case(m.get_name())
      args = m.get_arguments()
      c_type = []
      ml_type = []
      ml_args = []

      if m.is_method():
        c_type.append('t')
        ml_args.append('t__')
        ml_type.append('[> `%s ] gobject' %  snake_case(oname))

      for arg in args:
        is_opt = arg.is_optional()
        arg_type = c_ml_type(arg.get_type(), is_arg=True)
        if arg_type is None:
          raise ValueError('unhandled type for %s %s' % (arg, m))
        c_type.append(arg_type['c'])
        arg_name = arg.get_name()
        arg_type = arg_type['ml']
        if arg.may_be_null():
          arg_type = '?' + arg_name + ':' + arg_type
          arg_name = '?' + arg_name
        ml_args.append(arg_name)
        ml_type.append(arg_type)

      if m.can_throw_gerror():
        c_type.append('ptr (ptr GError.t)')

      if len(c_type) == 0:
        c_type.append('void')
        ml_args.append('()')
        ml_type.append('unit')

      return_base_type = None
      if m.is_constructor():
        c_type.append('returning t')
        ml_type.append('t')
        return_base_type = False
      else:
        return_type = c_ml_type(m.get_return_type(), is_arg=False)
        if return_type is None:
          raise ValueError('unhandled rt for ' + str(m))
        c_type.append('returning %s' % return_type['c'])
        ml_type.append(return_type['ml'])
        # TODO: check whether ownership is passed..., e.g. [m.get_caller_owns] ?
        return_base_type = return_type['base']

      c_type = ' @-> '.join(c_type)
      f_c.write('    let %s = foreign "%s"\n' % (mname, m.get_symbol()))
      f_c.write('      (%s)\n' % c_type)

      call_args = ' '.join(
          [m if m[0] != '?' else ('(match %s with | None -> null | Some v -> v)' % m[1:])
            for m in ml_args])
      ml_args = ' '.join([m for m in ml_args if m[0] == '?'] + [m for m in ml_args if m[0] != '?'])
      f_ml.write('  let %s %s =\n' % (mname, ml_args))
      if m.can_throw_gerror():
        f_ml.write('    let gerr__ = CArray.make (ptr C.GError.t) 1 in\n')
        f_ml.write('    let res = C.%s.%s %s (CArray.start gerr__) in\n' % (oname, mname, call_args))
        f_ml.write('    let gerr__ = CArray.get gerr__ 0 in\n')
        f_ml.write('    if not (Ctypes.is_null gerr__)\n')
        f_ml.write('    then begin\n')
        f_ml.write('      let msg = getf (!@ gerr__) C.GError.message in\n')
        f_ml.write('      if Ctypes.is_null msg\n');
        f_ml.write('      then failwith "failed with null error message";\n')
        f_ml.write('      let msg = C.strdup msg in\n');
        f_ml.write('      C.GError.free gerr__;\n')
        f_ml.write('      failwith msg\n')
        f_ml.write('    end;\n')
      else:
        f_ml.write('    let res = C.%s.%s %s in\n' % (oname, mname, call_args))

      if not return_base_type:
        f_ml.write('    if Ctypes.is_null res\n')
        f_ml.write('    then failwith "returned null";\n')
        f_ml.write('    Gc.finalise C.object_unref res;\n')
      f_ml.write('    res\n\n')

      ml_type = ' -> '.join([m for m in ml_type if m[0] == '?'] + [m for m in ml_type if m[0] != '?'])
      f_mli.write('  val %s : %s\n' % (mname, ml_type))
    except ValueError as e:
      if verbose: print(oname, m.get_name(), e)

  f_c.write('  end\n\n')
  f_ml.write('end\n\n')
  f_mli.write('end\n\n')

def write_files(f_c, f_ml, f_mli):
  f_c.write('(* THIS FILE IS AUTOMATICALLY GENERATED, DO NOT EDIT! *)\n\n')
  f_c.write('open Ctypes\n')
  f_c.write('module C (F : Cstubs.FOREIGN) = struct\n')
  f_c.write('  include Base_bindings.C(F)\n')
  f_c.write('  open F\n\n')

  f_ml.write('(* THIS FILE IS AUTOMATICALLY GENERATED, DO NOT EDIT! *)\n\n')
  f_ml.write('open Ctypes\n')
  f_ml.write('module C = Arrow_bindings.C (Arrow_generated)\n\n')
  f_ml.write('type _ gobject = C.gobject\n\n')

  f_mli.write('(* THIS FILE IS AUTOMATICALLY GENERATED, DO NOT EDIT! *)\n\n')
  f_mli.write('type _ gobject\n\n')
  for info in rep.get_infos('Arrow'):
    if isinstance(info, gi._gi.ObjectInfo):
      oname = info.get_name()
      type_name = snake_case(oname)
      f_c.write('  type %s = unit ptr\n' % type_name)
      f_c.write('  let %s : %s typ = ptr void\n' % (type_name, type_name))

  f_c.write('\n')
  f_ml.write('\n')
  f_mli.write('\n')

  for info in rep.get_infos('Arrow'):
    if isinstance(info, gi._gi.ObjectInfo) and not info.is_deprecated():
      handle_object_info(info, f_c, f_ml, f_mli)
  f_c.write('end')

with open('src/stubs/arrow_bindings.ml', 'w') as f_c:
  with open('src/wrappers/wrapper.ml', 'w') as f_ml:
    with open('src/wrappers/wrapper.mli', 'w') as f_mli:
      write_files(f_c, f_ml, f_mli)
