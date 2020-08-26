"""
HyperMorph Modules Testing:
(C) July 2020 By Athanassios I. Hatzis
"""
from hypermorph import MIS

# Load Schema
mis = MIS(debug=1, load=True, net_name='my_schema_binary', net_format='gt', net_path='/data/test/Schemata')

#
# =====================================================================================================
# Add ClickHouse databases as extra datasets on the fly with different API database client connections
# =====================================================================================================
#
mis.add('dataset', with_components=['tables', 'fields'], cname='USA Physicians', alias='Phys', ctype='CLICKHOUSE',
        descr='USA Physicians Compare National dataset',
        extra={'host': 'localhost', 'port': 9000, 'user': 'demo', 'password': 'demo',
               'db': 'TestDB', 'api': 'Clickhouse-Driver'})

fld_names = 'npi, pacID, profID, last, first, gender, school, graduated, spec0, spec1, org, groupID, groupTotal, ' \
            'city, state, phone, ccn1, lbn1, payCode, pqrs, ehr, hearts'

mis.get(502).get_rows().over(fld_names).where('graduated>2010').slice(10).to_dataframe().out()

feather_file = '/data/DemoData/Parquet/Physicians/Physician_Compare.fr'
mis.get(502).get_columns().over(fld_names).to_table(arrow_encoding=True).to_feather(feather_file).out()

#
# =====================================================================================================
# Add ClickHouse databases as extra datasets on the fly with different API database client connections
# =====================================================================================================
#
# New York Tax Bills
#
# Fast native connection using ClickHouse native Driver
mis.add('dataset', with_components=['tables', 'fields'],
        cname='NYC Property Tax Bills', alias='tax_bills_nyc', ctype='CLICKHOUSE',
        extra={'host': 'localhost', 'port': 9000, 'user': 'demo', 'password': 'demo',
               'db': 'DemoDB', 'api': 'Clickhouse-Driver'},
        descr='New York city property tax records')

mis.get(501).tables
mis.get(509).fields

# Change API connection on the fly
mis.get(501).get_connection(db_client='SQLAlchemy', port=8123)
mis.get(501).connection
mis.get(501).connection.sql('select * from tax_bills_nyc limit 3', out='tuples')

# *********************
# Eager evaluation
# *********************

fields = 'bbl, owner_name, tax_rate, emv, tbea'

# Pandas DataFrame
mis.get(509).get_rows().\
    over(fields).\
    slice(limit=500000, offset=200000).\
    to_dataframe(index='owner_name, bbl', trace=4).\
    out(lazy=False)
# 2.19sec

# Python Tuples
mis.get(509).get_rows().over(fields).slice(10, 2).to_tuples().out()

# PyArrow Table
mis.get(509).get_columns().over(fields).slice(10, 2).to_table().out()
mis.get(509).get_columns().over(fields, 'fld1, fld2, fld3, fld4, fld5').slice(10, 2).to_table().out()
mis.get(509).get_columns().over(fields, ['fld1', 'fld2', 'fld3', 'fld4', 'fld5']).slice(10, 2).to_table().out()

mis.get(509).get_columns().over(fields).slice(20, 2).to_batch(arrow_encoding=False).out()

mis.get(509).get_rows().over(fields).slice(20, 2).to_table().out()
mis.get(509).get_rows().over(fields).slice(20, 2).to_table(arrow_encoding=False).out()
mis.get(509).get_rows().over(fields).slice(20, 2).to_batch(arrow_encoding=False).out()

# Output to DataFrame is a lot faster from PyArrow
mis.get(509).get_rows().over(fields).slice(500000, 200000).to_table(arrow_encoding=False).to_dataframe().out()
# 720 msec


#
# ------------------------------------------------------------------------------------------------------------
#
# Using HTTP connection and SQLAlchemy (SLOW)
mis.add('dataset', with_components=['tables', 'fields'],
        cname='NYC Property Tax Bills', alias='tax_bills_nyc', ctype='CLICKHOUSE',
        extra={'host': 'localhost', 'port': 8123, 'user': 'demo', 'password': 'demo',
               'db': 'DemoDB', 'api': 'SQLAlchemy'},
        descr='New York city property tax records')

mis.get(565).tables

# Lazy evaluation
mis.get(552).\
    get_rows(partition_size=20000).\
    over(fields).\
    slice(limit=100000, offset=200000).\
    to_dataframe(index='owner_name, bbl', trace=4).\
    out(lazy=False)

mis.get(552).\
    get_rows(partition_size=20000).\
    over(fields).\
    slice(limit=100000, offset=200000).\
    to_dataframe(index='owner_name, bbl', trace=4).\
    out(lazy=True)
