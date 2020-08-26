"""
HyperMorph Modules Testing:
    Map selected fields of a table onto existing attributes of a datamodel

(C) August 2020 By Athanassios I. Hatzis
"""
from hypermorph import MIS

# Load Schema
mis = MIS(debug=1, load=True, net_name='my_schema_binary', net_format='gt', net_path='/data/test/Schemata')

mis.overview
mis.get(306).tables
mis.get(307).fields
select_fields = 'catsid, catpid, catcost, catqnt, catdate, catchk'

#
# Map Table to a NEW Entity and selected fields onto new attributes
#
cat = mis.get(307).get_fields().take(select_fields).to_entity('Catalog', 'CAT').out()
# Verify mapping
mis.get(307).get_fields().over('nid, dim4, dim3, dim2, cname, alias, vtype, attributes').to_dataframe().out()
# The new Entity with mapped field onto its attributes
cat.attributes

#
# Map Table to a NEW Entity and selected fields onto new attributes, with more parameters
#
as_names = 'sid, pid, price, quantity, inspection, check'
as_types = 'uint32, uint32, float32, uint32, timestamp[ms], bool'
cat2 = mis.get(307).get_fields().take(select_fields)\
          .to_entity('Catalog2', 'CAT2', datamodel_name='Foo Bar', as_types=as_types, as_names=as_names).out()

# Verify mapping
mis.get(307).get_fields().over('nid, dim4, dim3, dim2, cname, alias, vtype, attributes').to_dataframe().out()
# The new Entity with mapped field onto its attributes
cat2.attributes
cat2.to_hypergraph().out()

#
# map fields onto the attributes of a NEW Entity that is created in an existing DataModel,
#
prt_types = 'uint32, string, string, float32, string'
prt = mis.at(1, 847, 2).get_fields().to_entity('Part', 'PRT', datamodel=mis.at(2, 200, 0), as_types=prt_types).out()

#
# map fields of SQLite table onto the attributes of a NEW Entity that is created in an existing DataModel,
#
sup = mis.get(8).get_fields().to_entity('Supplier', 'SUP', datamodel=mis.at(2, 200, 0)).out()
