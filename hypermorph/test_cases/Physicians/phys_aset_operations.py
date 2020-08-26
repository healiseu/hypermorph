"""
HyperMorph Modules Testing:
    Add an Associative Entity Set (ASET) from an existing Entity
    The Entity has mapping, it is dictionary encoded and it is saved on disk with a feather format
    Associative Entity Set (ASET) operations:

(C) August 2020 By Athanassios I. Hatzis
"""
from hypermorph import MIS

# Load Schema
mis = MIS(debug=1, load=True, net_name='my_schema_binary', net_format='gt', net_path='/data/test/Schemata')
mis.at(2, 300, 0).components

# Add Clickhouse Physician Dataset from ClickHouse to compare results
phys_ch = mis.add('dataset',
                  with_components=['tables', 'fields'], cname='USA Physicians', alias='Phys', ctype='CLICKHOUSE',
                  descr='USA Physicians Compare National dataset',
                  extra={'host': 'localhost', 'port': 9000, 'user': 'demo', 'password': 'demo',
                         'db': 'TestDB', 'api': 'Clickhouse-Driver'})
chtbl = phys_ch.get_tables().to_nodes().out()[0]

#
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

# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
# ***************************************************************************************
#
# ASET operations
#
# ***************************************************************************************
# \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
cname_list = 'npi, pacID, spec0, groupTotal, last, first, gender, graduated, city, state, lbn1, ehr, pqrs'
as_names = 'id1, id2, specialty, total, last_name, first_name, sex, year, usa_city, usa_state, company, ehr, pqrs'

# ==================================================================
# Unfiltered
# ==================================================================

# Counting and Transformations
phys.reset()
phys.hbonds
phys.q.slice(10).out()
phys.q.slice(10).to_record_batch().out()
phys.q.slice(10).to_record_batch().to_records().out()
phys.q.slice(10).to_record_batch().to_table().out()
phys.q.slice(10).to_record_batch().to_dataframe().out()

# Transformations with projection and slicing
phys.q.over(cname_list).slice(10).out()
phys.q.over(cname_list).slice(10).to_record_batch().out()
phys.q.over(cname_list).slice(10).to_record_batch().to_table().out()
phys.q.over(cname_list).slice(10).to_record_batch().to_records().out()
phys.q.over(cname_list, as_names).slice(10).to_record_batch().to_dataframe().out()
phys.q.over(cname_list, as_names).slice(20).to_record_batch().\
    to_dataframe(index='id1, id2', order_by='usa_city, last_name, first_name').out()

# Wrapper, this is sorting and displaying all the rows using Pandas (slow for large size dataframes)
phys.print_rows(select=cname_list, order_by='city, last, first', limit=20, index='npi, pacID')

# ===================================================================
# Normal Filtering mode
# ===================================================================
phys.reset()
# 1st filter USA Physicians in Atlanta - 13199
# HyperMorph Wall time 250ms
phys.q.where('city like ATLANTA').filter().over(cname_list).to_record_batch().\
    to_dataframe(order_by='city, last, first', limit=20, index='npi, pacID').out()
# or break it into
phys.q.where('city like ATLANTA').filter().count().out()

#
# Compare with ClickHouse engine
# Wall time 420ms
#
chtbl.get_rows().over(cname_list).where("city like '%ATLANTA%'").order_by('last, first').slice(20).to_dataframe().out()

# 2nd filter Anesthesiologists in Atlanta - 1455 hbs
phys.q.where('spec0 like ANESTH').filter().count().out()
# 3rd filter Anesthesiologists in Atlanta graduated after 2010 - 499 hbs
phys.q.where('graduated>2010').filter().count().out()
# 4th filter Male Anesthesiologists in Atlanta graduated after 2010 - 298 hbs
phys.q.where('gender like F').filter().count().out()
# 5th filter Female Anesthesiologists in Atlanta graduated after 2010 that work in GRADY MEMORIAL HOSPITAL - 46 hbs
phys.q.where('lbn1 like GRADY').filter().count().out()
# 6th filter Female Anesthesiologists in Atlanta graduated after 2010 that work in GRADY MEMORIAL HOSPITAL
# and are registered in Medicare PQRS (Physician Quality Reporting System)
phys.q.where('pqrs like Y').filter().count().out()

phys.print_rows(select=cname_list, order_by='city, last, first', index='npi, pacID')

# Again with different order
cond1 = 'city like ATLANTA'
cond2 = 'spec0 like ANESTH'
cond3 = 'graduated>2010'
cond4 = 'gender like F'
cond5 = 'lbn1 like GRADY'
cond6 = 'pqrs like Y'
cond = "city like '%ATLANTA%' and spec0 like '%ANESTH%' and graduated>2010 and gender='F' and lbn1 like '%GRADY%' and pqrs='Y'"

phys.reset()
phys.q.where(cond2).filter().count().out()      # 180791
phys.q.where(cond4).filter().count().out()      #  76075
phys.q.where(cond3).filter().count().out()      #  23805
phys.q.where(cond6).filter().count().out()      #  14262
phys.q.where(cond1).filter().count().out()      #    117
phys.q.where(cond5).filter().count().out()      #     32

phys.print_rows(select=cname_list, order_by='city, last, first', index='npi, pacID')
# HACOL operations, update filtering state of HACOLs
phys.update_hacols_filtered_state()
phys.first.q.filter(phys.mask).count().out()
phys.first.q.filter(phys.mask).to_array(unique=True, order='asc').out()

# Warning you will get an error if you continue ASET filtering, use partial or full reset
# phys.reset(hacols_only=True)

#
# And again with conjuctive filtering criteria
#
def filter1():      # 1.71sec (it's slow because for each condition all rows are processed)
    phys.reset()
    phys.q.where(cond2).And(cond4).And(cond3).And(cond6).And(cond1).And(cond5).filter().count().out()

def filter2():      # 1.59sec (it's slow because for each condition all rows are processed)
    phys.reset()
    phys.q.where(cond1).And(cond2).And(cond3).And(cond4).And(cond5).And(cond6).filter().count().out()

def filter3():      # 525msec (ClickHouse is FAST for conjuctive conditions as expected)
    chtbl.get_rows().over(cname_list).where(cond).order_by('last, first').to_dataframe().out()

filter2()
phys.print_rows(select=cname_list, order_by='city, last, first', index='npi, pacID')

# ===================================================================
# Associative Filtering mode
# ===================================================================
phys.reset()
phys.select.where(cond2).filter().count().out()      # 180791 - 490ms
phys.select.where(cond4).filter().count().out()      #  76075 - 222ms
phys.select.where(cond3).filter().count().out()      #  23805 - 120ms
phys.select.where(cond6).filter().count().out()      #  14262 - 327ms
phys.select.where(cond1).filter().count().out()      #    117 - 432ms
phys.select.where(cond5).filter().count().out()      #     32 - 488ms
phys.print_rows(select=cname_list, order_by='city, last, first', index='npi, pacID')

phys.spec0.print_states()
phys.last.print_states()
phys.last.q.filter(phys.mask).count().out()
phys.last.q.to_array(unique=True, order='asc').out()
