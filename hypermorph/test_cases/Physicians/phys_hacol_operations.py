"""
HyperMorph Modules Testing:
    Add an Associative Entity Set (ASET) from an existing Entity
    The Entity has mapping, it is dictionary encoded and it is saved on disk with a feather format
    HyperAtomCollection (HACOL) operations:

(C) August 2020 By Athanassios I. Hatzis
"""
from hypermorph import MIS

# Load Schema
mis = MIS(debug=1, load=True, net_name='my_schema_binary', net_format='gt', net_path='/data/test/Schemata')
mis.at(2, 300, 0).components

# Add Associative Entity Set (ASET)
#
phys = mis.add('aset', entity=mis.at(2, 300, 1))
mis.mem.difference

# Loading ASET - Wall time 3.97 sec,
# time delay is due to reconstruction of HACOL states dictionaries,
# in the current release modified states and filtering state are not saved
#
#             Memory Usage
# Data             237.299
# Dictionary       111.819
# Total            349.118
#
# Memory allocated 237MB, measured also with `free -m` and `mis.mem.difference`
# Dictionary States are created with zero copy from Data
#
# Uncompressed size of data set
# 1.73 GB
# 2.78 million rows x 22 columns
#

phys.attributes
phys.data
phys.hacols
phys.memory_usage()

# HyperAtomCollection (HACOL) operations:
city = phys.city
gender = phys.gender
graduated = phys.graduated
spec0 = phys.spec0
lbn1 = phys.lbn1

print(city)
print(gender)
print(graduated)
print(spec0)
print(lbn1)

# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
# ***************************************************************************************
#
# HACOL operations in unfiltered State
#
# ***************************************************************************************
# \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
# Reset
city.reset()
# Memory Usage
city.memory_usage()
# Count atoms (values) - included, unique, instances, missing, total
city.count()
# States Dictionary
city.dictionary(limit=10, order_by='cnt, val', ascending=[False, True], index='cnt, val')

# Transformations with slicing and sorting
city.q.slice(20).to_array().out()
city.q.slice(20).to_array(order='asc').out()
city.q.slice(20).to_array(order='asc', unique=True).out()
city.q.slice(20).to_numpy().out()
city.q.slice(20).to_series().out()

# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
# ***************************************************************************************
#
# HACOL operations in filtered State
#
# ***************************************************************************************
# \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\

city.q.where('$v').like('ATL').filter().out()
city.count()
city.q.to_array(order='asc', unique=True).slice(20).out()
city.q.to_numpy(order='desc', limit=10).out()
city.q.to_series(order='desc', limit=10).out()

city.reset()
city.count()
