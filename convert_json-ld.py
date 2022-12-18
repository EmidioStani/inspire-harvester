from rdflib import Graph

g = Graph()
g.parse("output-4/1_5_1.iso19139-Switzerland.nt")
g.serialize(destination="example.jsonld", format="json-ld", context="https://raw.githubusercontent.com/SEMICeu/DCAT-AP/master/releases/2.1.0/dcat-ap_2.1.0.jsonld")
