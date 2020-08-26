"""
HyperMorph Modules Testing: get data from a flat file data resource
(C) August 2020 By Athanassios I. Hatzis
"""
from hypermorph import MIS

# Load Schema
mis = MIS(debug=1, load=True, net_name='my_schema_binary', net_format='gt', net_path='/data/test/Schemata')

# ===========================================================================================================
#
# Bike Trips CSV file
#
mis.get(220).tables
mis.get(222).get_rows().slice(20).to_dataframe().out()
mis.get(222).get_rows().over('Duration, Bike number, Member type, Start station number').slice(20).to_dataframe().out()
mis.get(222).get_rows().over('Duration, Bike number, Member type, Start station number').\
    slice(20).to_dataframe(index='Bike number').out()
mis.get(222).get_rows().slice(limit=20, offset=1000).to_dataframe().out()

# 5 dataframe partitions (chunks) with 20 records each
df_chunks = mis.get(222).get_rows(partition_size=20).slice(100).to_dataframe().out()
print([df_chunks.nrows, df_chunks.chunksize])
[df_chunks.get_chunk() for n in range(5)]

# ==========================================================================================================
#
# Get TSV datasets
mis.get_datasets().filter_view(attribute='ctype', value='TSV', reset=False).to_dataframe(index='nid').out()

# ==========================================================================================================
#
# Supplier-Part-Catalog TSV file
#
mis.get(306).tables

# to pandas dataframe
mis.get(307).get_rows().to_dataframe(nulls='\\N').out()
mis.get(307).get_rows().slice(limit=3, offset=10).to_dataframe(nulls='\\N').out()
mis.get(307).get_rows().\
    over(select='catsid, catpid, catcost, catqnt').\
    slice(limit=0, offset=0).to_dataframe(nulls='\\N', index='catsid, catpid').out()
mis.get(307).get_rows().over(select='sid, pid, price, quantity', as_names='sid, pid, price, quantity').\
    slice(limit=0, offset=1).to_dataframe(nulls='\\N', index='sid, pid').out()

#
# to pyarrow table structure or record batch with optional dictionary encoding
#
mis.get(307).get_rows().over().to_table(nulls=['\\N']).out()
mis.get(307).get_rows().over().to_batch(nulls=['\\N'], arrow_encoding=False).out()

mis.get(307).get_rows().\
    over(select='sid, pid, price, quantity', as_names='sid, pid, price, quantity, date, check, name').\
    to_batch(nulls=['\\N'], skip=1).out()
mis.get(307).get_rows().over(select='sid, pid, check, name', as_names='sid, pid, cost, quantity, date, check, name').\
    slice(limit=5, offset=9).to_table(nulls=['\\N'], skip=1).to_dataframe(index='sid, pid').out()

# with type conversion
as_names = 'sid, pid, cost, quantity, date, check, name'
as_types = 'uint32, uint32, float32, uint32, timestamp[ms], bool, string'
mis.get(307).get_rows().over(as_names=as_names, as_types=as_types).to_table(nulls=['\\N'], skip=1).out()

# ==========================================================================================================
#
# USA Physicians TSV file
#
mis.get(328).tables
col_names = ['npi', 'pacID', 'profID',
             'last', 'first', 'middle', 'suffix', 'gender', 'cred', 'school',
             'graduated',
             'spec0', 'spec1', 'spec2', 'spec3', 'spec4', 'specs',
             'org', 'groupID', 'groupTotal', 'address1', 'address2', 'address_marker',
             'city', 'state', 'zip', 'phone',
             'ccn1', 'lbn1', 'ccn2', 'lbn2', 'ccn3', 'lbn3', 'ccn4', 'lbn4', 'ccn5', 'lbn5',
             'pay_code', 'pqrs', 'ehr', 'hearts']

col_types = ['object', 'object', 'object',
             'category', 'category', 'category', 'category', 'category', 'category', 'category',
             'int64',
             'category', 'category', 'category', 'category', 'category', 'category',
             'category', 'category', 'int64', 'object', 'object', 'category',
             'category', 'category', 'object', 'object',
             'category', 'category', 'category', 'category', 'category',
             'category', 'category', 'category', 'category', 'category',
             'category', 'category', 'category', 'category']

mis.get(330).fields


# Read a sample file of 10 records with specific columns. Character '\\N' denotes NULL values
mis.get(330).get_rows().over().to_dataframe().out()
mis.get(330).get_rows().\
    over(select='FLD2, FLD3, FLD4, FLD5, FLD6, FLD9, FLD25, FLD26, FLD40, FLD41, FLD42').to_dataframe().out()
mis.get(330).get_rows().\
    over(select='npi, pacID, profID, last, first, gender, city, state, pqrs, ehr, hearts',
         as_names=col_names, as_types=col_types).slice(offset=1).to_dataframe(nulls='\\N').out()

mis.mem_diff
mis.mem.print_stats()

# Read 2780184 rows x 11 columns
df1 = mis.get(329).get_rows().\
    over(select='FLD2, FLD3, FLD4, FLD5, FLD6, FLD9, FLD25, FLD26, FLD40, FLD41, FLD42').to_dataframe(nulls='\\N').out()
# 12.5 sec
mis.mem_diff   # 583.4 MB
print(df1.dtypes)

# Read 2780184 rows x 11 columns with type conversion, it takes more time but less memory
df2 = mis.get(329).get_rows().\
    over(select='npi, pacID, profID, last, first, gender, city, state, pqrs, ehr, hearts',
         as_names=col_names, as_types=col_types).\
    to_dataframe(nulls='\\N').out()
# 19.8 sec
mis.mem_diff   # 453.4 MB
print(df2.dtypes)

# Read 2780184 rows x 11 columns with pyarrow.read_csv() method, fastest and memory efficient
mis.get(329).get_rows().over(select='FLD2, FLD3, FLD4, FLD5, FLD6, FLD9, FLD25, FLD26, FLD40, FLD41, FLD42').\
    to_table(nulls=['\\N']).out()
df3 = mis.get(329).get_rows().over(select='FLD2, FLD3, FLD4, FLD5, FLD6, FLD9, FLD25, FLD26, FLD40, FLD41, FLD42').\
    to_table(nulls=['\\N']).to_dataframe().out()
# 2.28 sec

mis.mem_diff   # 30 MB
print(df3.dtypes)
