"""
HyperMorph Modules Testing:
    testing clickhouse SQL queries with mymarilin/clickhouse-driver
(C) Jyly 2020 By Athanassios I. Hatzis
"""
from hypermorph.clients import ConnectionPool

ch = ConnectionPool(db_client='Clickhouse-Driver', host='localhost', port=9000,
                    user='demo', password='demo', database='TriaDB', trace=3)

# sql queries
q = 'SELECT s_id, p_id, c_price, c_quantity, c_date, c_check FROM DAT_242_1'
ch.sql(q)
ch.sql(q, execute=False)
ch.sql(q, index='s_id, p_id', trace=3)
ch.sql('SELECT s_id, p_id, c_price, c_quantity, c_date FROM DAT_242_1',
       as_columns='supplierID, partID, catalog price, catalog quantity, catalog date', index='supplierID, partID')

ch.sql(q, out='tuples', trace=3, qid='SELECT from catalog')
ch.sql('SELECT s_id, p_id, c_price FROM DAT_242_1 LIMIT 5', out='tuples', trace=3)

ch.sql('SELECT s_id, p_id, c_price FROM DAT_242_1 LIMIT 5', out='json_rows', trace=3)
ch.sql('SELECT s_id, p_id, c_price FROM DAT_242_1 LIMIT 5', out='json_rows', trace=3,
       as_columns='supplierID, partID, catalog price, catalog quantity, catalog date')

ch.sql('SELECT s_id, p_id, c_price FROM DAT_242_1 LIMIT 5', out='columns', trace=3)
ch.sql('SELECT s_id, p_id, c_price FROM DAT_242_1 LIMIT 5', out='table', trace=3)
ch.sql('SELECT c_quantity FROM DAT_242_1', out='batch', trace=3)
ch.sql('SELECT c_quantity FROM DAT_242_1', out='batch', arrow_encoding=False, trace=3)

ch.sql('SELECT * FROM DAT_242_1', out='table')
ch.sql('select s_id, p_id, c_price, c_date from DAT_242_1', out='table')
ch.sql('select s_id, p_id, c_price, c_date from DAT_242_1', out='batch')
ch.sql('select s_id, p_id, c_price, c_date from DAT_242_1', out='table', as_columns='sid, pid, price, date')
ch.sql('select s_id, p_id, c_price, c_date from DAT_242_1', out='table', as_columns=['sid', 'pid', 'price', 'date'])

ch.sql('SELECT * FROM DAT_242_1', out='table', arrow_encoding=False)
ch.sql('SELECT * FROM DAT_242_1', out='batch', arrow_encoding=False)

#
# get_tables()
#
ch.get_tables_metadata(trace=3)
ch.get_tables_metadata(name='200', qid='Table Engines Metadata')
ch.get_tables_metadata(name='200', execute=False, trace=3)
ch.get_tables_metadata(fields='*')
ch.get_tables_metadata(fields='name as table, create_table_query', clickhouse_engine='MergeTree')
ch.get_tables_metadata(clickhouse_engine='MergeTree')
ch.get_tables_metadata(clickhouse_engine='MergeTree', name='DAT')
ch.get_tables_metadata(clickhouse_engine='MergeTree', name='DAT',
                       fields='name as table, partition_key as part_key, sorting_key as skey, primary_key as pkey')

#
# get_columns()
#
ch.get_columns_metadata()
ch.get_columns_metadata(fields='*')
ch.get_columns_metadata(table='HLink_200')
ch.get_columns_metadata(table='HLink_500', aggr=True)
ch.connector.get_columns(table='HLink_500', index='name',
                        fields='name, type, '
                               'is_in_partition_key as in_part_key, '
                               'is_in_sorting_key as in_skey, '
                               'is_in_primary_key as in_pkey')

ch.get_columns_metadata(table='DAT_363_3')
ch.get_columns_metadata(table='DAT_363_3', aggr=True)
ch.get_columns_metadata(table='DAT_363_3', fields='name, type, is_in_partition_key as in_part_key', index='name')
ch.get_columns_metadata(table='DAT_242_1', columns=['c_quantity', 'c_price', 'c_check'], fields='name, type')

# ================================================================================================================
# other commands that work ONLY with clickhouse-driver connector, i.e. are not abstracted at ConnectionPool level
# ================================================================================================================
# Create Memory Engine
ch.connector.create_engine(table='MEM', engine='Memory', heading=['pk Int32', 'val Nullable(UInt16)'], execute=True)

# Create MergeTree Engine
keys = [('impdate', 'Date'),
        ('rowno', 'UInt32')]

dtypes = [
 ('npi', 'UInt64'),
 ('pacID', 'UInt64'),
 ('profID', 'String'),
 ('last', 'LowCardinality(Nullable(String))'),
 ('first', 'LowCardinality(Nullable(String))'),
 ('gender', 'LowCardinality(Nullable(String))'),
 ('graduated', 'UInt16'),
 ('city', 'LowCardinality(Nullable(String))'),
 ('state', 'LowCardinality(Nullable(String))'),
 ('pqrs', 'LowCardinality(Nullable(String))'),
 ('ehr', 'LowCardinality(Nullable(String))'),
 ('hearts', 'LowCardinality(Nullable(String))')
]

ch.connector.create_engine(table='Physician2', engine='MergeTree',
                           heading=[f'{pair[0]} {pair[1]}' for pair in keys+dtypes],
                           partition_key='impdate', order_key='(impdate, rowno)',
                           settings='old_parts_lifetime = 30', execute=True)


"""
SELECT npi, pacID, profID, last, first, gender, graduated, city, state, pqrs, ehr, hearts
FROM file(
   '/dbstore/clickhouse/user_files/athan/FlatFiles/Physicians/Physicians_Compare.tsv',
   'TabSeparatedWithNames',
   'npi UInt64, 
    pacID UInt64, 
    profID String, 
    last Nullable(String), 
    first Nullable(String), 
    middle Nullable(String), 
    suffix Nullable(String), 
    gender Nullable(String), 
    cred Nullable(String), 
    school Nullable(String), 
    graduated Nullable(UInt16), 
    spec0 Nullable(String), 
    spec1 Nullable(String), 
    spec2 Nullable(String), 
    spec3 Nullable(String), 
    spec4 Nullable(String), 
    specs Nullable(String), 
    org Nullable(String), 
    groupID Nullable(String), 
    groupTotal Nullable(UInt32), 
    address1 Nullable(String), 
    address2 Nullable(String), 
    addrMarker Nullable(String), 
    city Nullable(String), 
    state Nullable(String), 
    zip Nullable(String), 
    phone Nullable(String), 
    ccn1 Nullable(String), 
    lbn1 Nullable(String), 
    ccn2 Nullable(String), 
    lbn2 Nullable(String), 
    ccn3 Nullable(String), 
    lbn3 Nullable(String), 
    ccn4 Nullable(String), 
    lbn4 Nullable(String), 
    ccn5 Nullable(String), 
    lbn5 Nullable(String), 
    payCode Nullable(String), 
    pqrs Nullable(String), 
    ehr Nullable(String), 
    hearts Nullable(String)'
)
"""


ch.connector.get_parts(table='HAtom_200')
ch.connector.get_parts(table='HAtom_200States', hb2=12, active=True, execute=True)
ch.connector.get_parts(table='HLink_200', active=True, execute=True)
ch.connector.get_parts(table='DAT_242_1', active=True, execute=True)
ch.connector.get_parts(table='HAtom_200_Float32', active=True, execute=True)

ch.connector.optimize_engine(table='HAtom_200', execute=True)

ch.connector.last_query_statistics
ch.connector.print_query_statistics
