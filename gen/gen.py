import xml.etree.ElementTree as ET
tree = ET.parse('gen/Arrow-1.0.gir')
root = tree.getroot()
for child in root:
  print(child.tag, child.attrib)
