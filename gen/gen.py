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
o = rep.get_infos('Arrow')
c = o[0].get_methods()
count = c[1]
rt = count.get_return_type()
print(rt.get_tag_as_string())
