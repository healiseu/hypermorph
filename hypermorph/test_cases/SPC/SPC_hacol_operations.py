"""
HyperMorph Modules Testing:
    Add an Associative Entity Set (ASET) from an existing Entity
    The Entity has mapping, it is dictionary encoded and it is saved on disk with a feather format
    HyperAtomCollection (HACOL) operations:

(C) August 2020 By Athanassios I. Hatzis
"""
from hypermorph import MIS
mis = MIS(debug=1, load=True, net_name='my_schema_binary', net_format='gt', net_path='/data/test/Schemata')

# ************************************************************************
# Numerical Collections
# ************************************************************************
mis.at(2, 200, 1).attributes
acat = mis.add('aset', entity=mis.at(2, 200, 1))

# -----------------------------------------------------------------------
# Catalog quantity (table column)
# -----------------------------------------------------------------------
acat.data                                    # Chunked Array dictionary encoded
acat.hacols
print(acat.quantity)
# Memory Usage
acat.quantity.memory_usage(mb=False)

# Transformations
acat.quantity.data
acat.quantity.q.to_series().out()                          # Pandas Series
acat.quantity.q.to_array().out()                           # PyArrow Array
acat.quantity.q.to_array(unique=True, order='asc').out()   # PyArrow Array unique values and order
acat.quantity.q.to_numpy(order='asc').out()                # NumPy Array

# Counting hatoms, values
acat.quantity.count()

# Restriction with condition,
acat.quantity.q.where('$v>=200').out()            # BooleanArray Mask
acat.quantity

# Filtering
acat.quantity.reset()
acat.quantity.q.where('$v>=200').filter().count().out()

acat.quantity.reset()
acat.quantity.q.where('$v>=200').filter()
acat.quantity.count()
# Filtering with Transformations
acat.quantity.q.to_series().out()              # Pandas Series
acat.quantity.q.to_array().out()               # PyArrow Array
acat.quantity.q.to_numpy().out()               # NumPy Array

# -----------------------------------------------------------------------
# Catalog Price (table column)
# -----------------------------------------------------------------------
acat.price.data
print(acat.price)

# Transformations
acat.price.q.to_series().out()   # Pandas Series
acat.price.q.to_array().out()    # PyArrow Array
acat.price.q.to_numpy().out()    # Numpy Array

# Filtering with Transformations
acat.price.reset()
acat.price.q.where('$v<20').filter().to_series(order='desc').out()

# Filtering with Counting
acat.price.reset()
acat.price.q.where('$v<20').filter().count().out()

# Filtering with value that is out of bounds
acat.price.reset()
acat.price.q.where('$v>150').filter().out()
acat.price.reset()
acat.price.q.where('$v>150').filter().to_series().out()
acat.price.reset()
acat.price.q.where('$v>150').filter().to_array().out()

# States Dictionary with sorting
acat.price.dictionary()
acat.price.dictionary(order_by='cnt, val', ascending=[False, False], index='cnt, val')
acat.price.print_states()

# ************************************************************************
# String Collections
# ************************************************************************
aprt = mis.add('aset', entity=mis.at(2, 200, 8))
print(aprt.pname)

# Transformations
aprt.pname.data
aprt.pname.q.to_series().out()              # Pandas Series
aprt.pname.q.to_numpy().out()               # Numpy Array
aprt.pname.q.to_array(unique=True).out()    # PyArrow Array
# Dictionary
aprt.pname.print_states()

# Filtering with Transformations
aprt.pname.q.where('$v').like('asher').filter().to_array().out()
aprt.pcolor.q.where('$v').like('Red').filter().to_series().out()
