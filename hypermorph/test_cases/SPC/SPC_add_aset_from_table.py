"""
HyperMorph Modules Testing:
    Add three Associative Entity Sets (ASET)
    by mapping Field(s) from different data resources (two flat files and a MySQL table) on Attributes of a Data Model
    The NEW ASETs are encoded and saved, the updated schema is also saved

(C) August 2020 By Athanassios I. Hatzis
"""
from hypermorph import MIS

# Load Schema
mis = MIS(debug=1, load=True, net_name='my_schema_binary', net_format='gt', net_path='/data/test/Schemata')

# Add an Associative Entity Set (ASET) - Case 3
# Create a NEW Entity and a NEW DataModel by mapping selected fields from a flat file onto NEW Attributes
acat_fields = 'catsid, catpid, catcost, catqnt, catdate, catchk'
acat_names = 'sid, pid, price, quantity, inspection, check'
acat_types = 'uint32, uint32, float32, uint32, timestamp[ms], bool'
acat = mis.add('aset', from_table=mis.at(1, 847, 1), with_fields=acat_fields,
               datamodel_name='MY SPC', datamodel_alias='MY_SPC',
               entity_name='Catalog', entity_alias='CAT', as_names=acat_names, as_types=acat_types)

# Add an Associative Entity Set (ASET) - Case 2
# Create a NEW Entity in an existing DataModel by mapping all fields of a flat file onto NEW Attributes
aprt_types = 'uint32, string, string, float32, string'
aprt = mis.add('aset', from_table=mis.at(1, 847, 2), datamodel=mis.at(2, 200, 0),
               entity_name='Part', entity_alias='PRT', as_types=aprt_types)

# Add an Associative Entity Set (ASET) - Case 2
# Create a NEW Entity Set in an existing DataModel by mapping all fields of a MySQL table onto NEW Attributes
asup = mis.add('aset', from_table=mis.get(115), datamodel=mis.at(2, 200, 0), entity_name='Supplier', entity_alias='SUP',
               as_names='s_id, s_name, s_address, s_country, s_city, s_status')

# Apply dictionary encoding on each ASET
acat.dictionary_encode(nulls=['\\N'], skip=1)
acat.attributes
acat.data
acat.hacols
acat.memory_usage()

aprt.dictionary_encode(nulls=['\\N'], skip=1)
aprt.attributes
aprt.data
aprt.hacols

asup.dictionary_encode()
asup.attributes
aprt.data
aprt.hacols

# Save Schema
mis.save()

