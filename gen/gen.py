import os
import re
os.environ['GI_TYPELIB_PATH'] = 'gen'

type_names = {
    'array': 'array_',
}

func_names = {
    'new': 'new_',
}

ctypes = {
    'gint64': 'int64_t',
    'gdouble': 'double',
    'gfloat': 'float',
    'gboolean': 'bool',
}

def snake_case(name):
  s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
  res = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
  return type_names.get(res, res)

import xml.etree.ElementTree as ET
tree = ET.parse('gen/Arrow-1.0.gir')
root = tree.getroot()
for child in root:
  print(child.tag, child.attrib)

import gi
gi.require_version('Arrow', '1.0')
from gi.repository import Arrow
rep = gi.Repository.get_default()

def ctype(type_):
  t = type_.get_tag_as_string()
  ctype = ctypes.get(t, None)
  if ctype is not None: return ctype
  if t == 'interface':
    return snake_case(type_.get_interface().get_name())

def handle_object_info(oinfo, fobj):
  oname = oinfo.get_name()
  # ctype: oinfo.get_type_name()
  fobj.write('  module %s = struct\n' % oname)
  fobj.write('    type t = %s\n\n' % snake_case(oname))

  for m in oinfo.get_methods():
    if m.is_deprecated(): continue
    try:
      mname = m.get_name()
      mname = func_names.get(mname, mname)
      args = m.get_arguments()
      type_ = []
      if m.is_method(): type_.append('t')
      for arg in args:
        is_opt = arg.is_optional()
        arg_type = ctype(arg.get_type())
        if arg_type is None:
          raise ValueError('unhandled type for %s %s' % (arg, m))
        type_.append(arg_type)
      if m.can_throw_gerror():
        type_.append('ptr ptr void')
      if m.is_constructor():
        type_.append('returning t')
      else:
        rt = ctype(m.get_return_type())
        if rt is None:
          raise ValueError('unhandled rt for ' + str(m))
        type_.append('returning %s' % rt)
      type_ = ' @-> '.join(type_)
      fobj.write('    let %s = foreign "%s"\n' % (mname, m.get_symbol()))
      fobj.write('      (%s)\n' % type_)
    except ValueError as e:
      print(e)

  fobj.write('  end\n\n')

  if oname.startswith('Array'):
    print('>>', oinfo, oinfo.get_name(), oinfo.get_type_name())
    for method in oinfo.get_methods():
      can_throw = method.can_throw_gerror()
      args = method.get_arguments()
      for arg in args:
        is_opt = arg.is_optional()
        arg_type = arg.get_type()
        t = arg_type.get_tag_as_string()
        i = arg_type.get_interface()
        # if == 'interface' => t.get_interface()
      rt = method.get_return_type()
      print('  ', method.get_name(), method.get_symbol(), len(args), rt.get_tag_as_string(), rt.get_interface())

with open('src/stubs/stubs_generated.ml', 'w') as fobj:
  fobj.write('open Ctypes\n')
  fobj.write('module C (F : Cstubs.FOREIGN) = struct\n')
  fobj.write('  open F\n')

  for info in rep.get_infos('Arrow'):
    if isinstance(info, gi._gi.ObjectInfo):
      type_name = snake_case(info.get_name())
      fobj.write('  type %s = unit ptr\n' % type_name)
      fobj.write('  let %s : %s typ = ptr void\n' % (type_name, type_name))

  fobj.write('\n')
  for info in rep.get_infos('Arrow'):
    if isinstance(info, gi._gi.ObjectInfo):
      handle_object_info(info, fobj)
  fobj.write('end')
