# The code generated relies on inheritance working without
# applying any macros just by casting the pointer.
#
# TODO: check that it is the case.
# TODO: list/array support, e.g. for the result [get_values].
# TODO: rewrite in ocaml ?
import os
import re
os.environ['GI_TYPELIB_PATH'] = 'gen'

verbose = True

snake_exceptions = {
    'array': 'array_',
    'new': 'new_',
}

base_types = {
    'gint64': { 'c': 'int64_t', 'ml': 'Int64.t' },
    'gint32': { 'c': 'int32_t', 'ml': 'Int32.t' },
    'gint16': { 'c': 'int16_t', 'ml': 'int' },
    'gint8': { 'c': 'int8_t', 'ml': 'int' },
    'guint64': { 'c': 'uint64_t', 'ml': 'Unsigned.uint64' },
    'guint32': { 'c': 'uint32_t', 'ml': 'Unsigned.uint32' },
    'guint16': { 'c': 'uint16_t', 'ml': 'Unsigned.uint16' },
    'guint8': { 'c': 'uint8_t', 'ml': 'Unsigned.uint8' },
    'gdouble': { 'c': 'double', 'ml': 'float' },
    'gfloat': { 'c': 'float', 'ml': 'float' },
    'gboolean': { 'c': 'bool', 'ml': 'bool' },
    'utf8': { 'c': 'string', 'ml': 'string' },
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

class Type(object):
  def __init__(self, type_, arg=None):
    t = type_.get_tag_as_string()
    base_type = base_types.get(t, None)
    self._len = None
    if arg is not None:
      self._arg_name = arg.get_name()
      self._nullable = bool(arg.may_be_null())
    if base_type is not None:
      self._c = base_type['c']
      self._ml = base_type['ml']
      self._base = True
      return
    self._base = False
    if arg is not None and t == 'array':
      l = type_.get_array_length()
      elt_type = base_types.get(type_.get_param_type(0).get_tag_as_string(), None)
      if elt_type is not None:
        self._elt_c = elt_type['c']
        self._c = 'ptr ' + elt_type['c']
        self._ml = elt_type['ml'] + ' list'
        self._len = l
        self._nullable = False
        return
    if t == 'interface':
      oname = type_.get_interface().get_name()
      t = snake_case(oname)
      if t not in unsupported_types:
        if arg is not None: self._ml = '[> `%s ] gobject' % t
        else: self._ml = variant_type(type_.get_interface())
        self._c = t
        return
    raise ValueError('unhandled type %s %s' % (type_, arg))

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
      arg_types = []
      len_indexes = {}

      for arg in args:
        arg_type = Type(arg.get_type(), arg=arg)
        arg_types.append(arg_type)
        if arg_type._len is not None:
          len_indexes[arg_type._len] = arg.get_name()

      no_arg = len(args) == 0 and not m.can_throw_gerror() and not m.is_method()

      return_type = None
      if not m.is_constructor():
        return_type = Type(m.get_return_type(), arg=None)

      call_args = ['t__'] if m.is_method() else []

      for index, arg in enumerate(arg_types):
        if index in len_indexes:
          of_int = None
          if arg._ml == 'Int64.t': of_int = 'Int64.of_int'
          if arg._ml == 'Int32.t': of_int = 'Int32.of_int'
          if arg._ml == 'Unsigned.uint32': of_int = 'Unsigned.UInt32.of_int'
          if arg._ml == 'Unsigned.uint64': of_int = 'Unsigned.UInt64.of_int'
          if of_int is None:
            raise ValueError('cannot convert to int ' + str(arg))
          call_args.append('(List.length %s |> %s)' % (len_indexes[index], of_int))
          continue
        if arg._nullable: arg = '(match %s with | None -> null | Some v -> v)' % arg._arg_name
        elif arg._len is not None:
          arg = '(CArray.of_list %s %s |> CArray.start)' % (arg._elt_c, arg._arg_name)
        else: arg = arg._arg_name
        call_args.append(arg)

      if no_arg: call_args.append('()')
      call_args = ' '.join(call_args)

      c_type = [ 't' ] if m.is_method() else []
      c_type += [ t._c for t in arg_types ]
      if m.can_throw_gerror(): c_type.append('ptr (ptr GError.t)')
      if no_arg: c_type.append('void')
      if m.is_constructor(): c_type.append('returning t')
      else: c_type.append('returning %s' % return_type._c)

      f_c.write('    let %s = foreign "%s"\n' % (mname, m.get_symbol()))
      f_c.write('      (%s)\n' % ' @-> '.join(c_type))

      ml_args = ['?' + m._arg_name for m in arg_types if m._nullable]
      if m.is_method(): ml_args.append('t__')
      ml_args += [m._arg_name for i, m in enumerate(arg_types) if not m._nullable and i not in len_indexes]
      if len(ml_args) == 0: ml_args = ['()']
      f_ml.write('  let %s %s =\n' % (mname, ' '.join(ml_args)))
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

      if m.is_constructor() or (return_type is not None and not return_type._base):
        f_ml.write('    if Ctypes.is_null res\n')
        f_ml.write('    then failwith "returned null";\n')
        f_ml.write('    Gc.finalise C.object_unref res;\n')
      f_ml.write('    res\n\n')

      ml_type = ['?' + m._arg_name + ':' + m._ml for m in arg_types if m._nullable]
      if m.is_method(): ml_type += ['[> `%s ] gobject' %  snake_case(oname)]
      ml_type += [m._ml for i, m in enumerate(arg_types) if not m._nullable and i not in len_indexes]
      if no_arg: ml_type.append('unit')
      if m.is_constructor(): ml_type.append('t')
      else: ml_type.append(return_type._ml)

      f_mli.write('  val %s : %s\n' % (mname, ' -> '.join(ml_type)))
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
