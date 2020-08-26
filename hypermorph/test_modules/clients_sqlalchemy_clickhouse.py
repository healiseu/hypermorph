"""
HyperMorph Modules Testing:
    testing clickhouse queries with sqlalchemy-clickhouse python client
(C) August 2020 By Athanassios I. Hatzis
"""
from hypermorph.clients import ConnectionPool

ch = ConnectionPool(db_client='SQLAlchemy', dialect='clickhouse',
                    host='localhost', port=8123, user='demo', password='demo', database='TriaDB', trace=3)

ch.sql('SELECT rowno, e_last, e_first, e_birth, e_city, e_country FROM DAT_363_3')
ch.sql('SELECT rowno, e_last, e_first, e_birth, e_city, e_country FROM DAT_363_3',
       as_columns='Last, First, BirthDate, City, Country', index='rowno')
ch.sql('SELECT rowno, e_last, e_first, e_birth, e_city, e_country FROM DAT_363_3', index='rowno', out='tuples')
ch.sql('SELECT rowno, e_last, e_first, e_birth, e_city, e_country FROM DAT_363_3', index='rowno', out='json_rows')
ch.sql('SELECT rowno, e_last, e_first, e_birth, e_city, e_country FROM DAT_363_3',
       as_columns='ID, Last, First, BirthDate, City, Country', index='rowno', out='json_rows')

ch.get_tables_metadata()
ch.get_tables_metadata(fields='*')
ch.get_tables_metadata(fields='name as table, create_table_query', clickhouse_engine='MergeTree')
ch.get_tables_metadata(clickhouse_engine='MergeTree')
ch.get_tables_metadata(clickhouse_engine='MergeTree',
                       fields='name as table, partition_key as part_key, sorting_key as skey, primary_key as pkey')

ch.get_columns_metadata()
ch.get_columns_metadata(fields='*')
ch.get_columns_metadata(table='DAT_363_3')
ch.get_columns_metadata(table='DAT_363_3', fields='name, type, is_in_partition_key as in_part_key', index='name')
ch.get_columns_metadata(table='DAT_363_3', aggr=True)

ch.get_columns_metadata(table='HLink_500')
ch.get_columns_metadata(table='HLink_500', aggr=True)
ch.get_columns_metadata(table='HLink_500', index='name',
                        fields='name, type, '
                               'is_in_partition_key as in_part_key, '
                               'is_in_sorting_key as in_skey, '
                               'is_in_primary_key as in_pkey')
