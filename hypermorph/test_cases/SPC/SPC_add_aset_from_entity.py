"""
HyperMorph Modules Testing:
    Add Associative Entity Sets(ASETs) from existing Entities
    The Entities have mapping, they are dictionary encoded and their data are saved on disk with a feather format

(C) July 2020 By Athanassios I. Hatzis
"""
from hypermorph import MIS

# Load Schema
mis = MIS(debug=1, load=True, net_name='my_schema_binary', net_format='gt', net_path='/data/test/Schemata')


# The Entity with the mapping
mis.at(2, 200, 0).entities
# Its attributes
mis.at(2, 200, 14).attributes

#
# Add an Associative Entity Set (ASET) - Case 1
#
acat = mis.add('aset', entity=mis.at(2, 200, 1))

acat.attributes
acat.data
acat.data.to_pandas()
acat.hacols
acat.memory_usage()

asup = mis.add('aset', entity=mis.at(2, 200, 14))
asup.attributes
