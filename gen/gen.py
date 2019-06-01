import os
os.environ['GI_TYPELIB_PATH'] = 'gen'

import xml.etree.ElementTree as ET
tree = ET.parse('gen/Arrow-1.0.gir')
root = tree.getroot()
for child in root:
  print(child.tag, child.attrib)

# set GI_TYPELIB_PATH
import gi
gi.require_version('Arrow', '1.0')
from gi.repository import Arrow
rep = gi.Repository.get_default()

for elem in rep.get_infos('Arrow'):
  if not isinstance(elem, gi._gi.ObjectInfo): continue
  print('>>', elem, elem.get_name(), elem.get_type_name())
  for method in elem.get_methods():
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
