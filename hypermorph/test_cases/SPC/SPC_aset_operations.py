"""
HyperMorph Modules Testing:
    Add an Associative Entity Set (ASET) from an existing Entity
    The Entity has mapping, it is dictionary encoded and it is saved on disk with a feather format
    Associative Entity Set (ASET) operations:

(C) August 2020 By Athanassios I. Hatzis
"""
from hypermorph import MIS
mis = MIS(debug=1, load=True, net_name='my_schema_binary', net_format='gt', net_path='/data/test/Schemata')
mis.at(2, 200, 0).entities
acat = mis.add('aset', entity=mis.at(2, 200, 1))

# Transformation
column_names = acat.entity.get_attributes().over('cname').to_tuples().out()
acat.q.over(column_names).to_dataframe(index='catsid, catpid').out()
acat.print_rows(select=column_names, index='catsid, catpid')

# Memory Usage
acat.memory_usage(mb=False)

# Testing masks
acat.reset()
acat.mask
acat.data.column(acat.dim2_to_col_pos[5])
acat.q.where('$5').out()
acat.q.where('quantity').out()
acat.q.where('$5 >= 200').out()
acat.q.where('quantity >= 200').out()

acat.quantity.q.to_array().out()
acat.q.where('quantity>=200').And('price<20').out()

# ===================================================================
# Normal Filtering mode
# ===================================================================
acat.reset()
acat.q.where('quantity>=200').filter().count().out()
acat.print_rows(select=column_names, index='catsid, catpid')

# Pass the filtering to HACOLs
acat.update_hacols_filtered_state()
acat.quantity.count()
acat.catpid.count()

# Partial reset of HACOLs to continue filtering otherwise you will get an error...
# when you try to filter ASET with
acat.reset(hacols_only=True)

acat.q.where('price<20').filter().count().out()
acat.print_rows(select=column_names, index='catsid, catpid')

# Normal Filtering mode with conjuctive conditions
acat.reset()
acat.q.where('quantity>=200').And('price<20').filter().count().out()
acat.print_rows(select=column_names, index='catsid, catpid')

acat.update_hacols_filtered_state()
acat.quantity.count()
acat.catpid.count()

# Retrospection
print(acat)
acat.hbonds
acat.is_filtered()

# ==================================================================
# Associative Filtering Mode
# ==================================================================
acat.reset()
# 1st filter
acat.select.where('price<20').filter().count().out()
# Examine rows of a table
acat.print_rows(select=column_names, index='catsid, catpid')
# Examine dictionary states for three columns
acat.price.print_states()
acat.quantity.print_states()
acat.catpid.print_states()

# 2nd filter
acat.select.where('quantity>=200').filter().count().out()
# Examine rows of a table
acat.print_rows(select=column_names, index='catsid, catpid')
# Examine dictionary states for three columns
acat.price.print_states()
acat.quantity.print_states()
acat.catpid.print_states()

# Conjuctive filtering
acat.reset()
acat.select.where('quantity>=200').And('price<20').filter().count().out()
# Examine rows of a table
acat.print_rows(select=column_names, index='catsid, catpid')
# Examine dictionary states for three columns
acat.price.print_states()
acat.quantity.print_states()
acat.catpid.print_states()
