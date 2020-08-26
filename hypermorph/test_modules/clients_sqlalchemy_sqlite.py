"""
HyperMorph Modules Testing:
    testing sqlite queries with SQLAlchemy python client
(C) August 2020 By Athanassios I. Hatzis
"""

import pandas as pd
from hypermorph.clients import ConnectionPool

# Test sqlite sqlalchemy client with database file
client = ConnectionPool(db_client='SQLAlchemy', dialect='sqlite', path='/data/DataSQLite/spc.db')

# Fetch all records from database table use the default output = `dataframe`
#
# Select all columns from a database table
client.sql('SELECT * FROM Parts')

client.sql('SELECT * FROM Parts', out='tuples')
client.sql('SELECT * FROM Parts', out='json_rows')

# PyArrow transformations
client.sql('SELECT pid, pcolor, pweight from Parts', as_columns='partID, partName, partWeight', out='table')
client.sql('SELECT pid, pcolor, pweight from Parts', as_columns=['partID', 'partName', 'partWeight'], out='batch')

client.sql('SELECT pid, pcolor, pweight from Parts', as_columns='partID, partName, partWeight',
           out='batch', arrow_encoding=False)
client.sql('SELECT pid, pcolor, pweight from Parts', as_columns='partID, partName, partWeight',
           out='batch', arrow_encoding=True)

# Select specific columns
client.sql('SELECT pid, pname, pweight FROM Parts')
client.sql('SELECT pid, pname, pweight, punit FROM Parts', index='pid, pname')
client.sql('SELECT pid, pname, pweight, punit FROM Parts', index=['pid', 'pname'])
client.sql("SELECT pid, pweight, punit FROM Parts WHERE punit='lb'")
# Disable execution
client.sql('SELECT * FROM Parts', execute=False)

# Test DBAPIError Exceptions
client.sql('SELECT name FOM Persons')
client.sql('SELECT pid, pname, pweight FROM Persons')

# ALL metadata from sqlite_master
client.sql('SELECT * FROM sqlite_master')

# tables of sqlite database with specific metadata columns
client.sql("SELECT name, tbl_name, sql FROM sqlite_master WHERE type='table'")

# Get all metadata for the columns of a table
client.sql('PRAGMA table_info(Parts)')

# Use the cmd() method to read meta-data
#
# tables
client.get_tables_metadata()
client.get_tables_metadata(fields='tbl_name')
client.get_tables_metadata(fields='tbl_name, sql', execute=False, trace=3)
client.get_tables_metadata(fields='tbl_name, sql', index='tbl_name')
client.get_tables_metadata(fields='tbl_name', out='tuples')
table_names = [tbl[0] for tbl in client.get_tables_metadata(fields='tbl_name', out='tuples')]
print(table_names)

# columns
client.get_columns_metadata()
client.get_columns_metadata(out='tuples')
client.get_columns_metadata(table='Parts')
client.get_columns_metadata(table='Parts', fields='name, type')
client.get_columns_metadata(table='Parts', out='tuples')
client.get_columns_metadata(table='Parts', out='json_rows')
print([col[1:3] for col in client.get_columns_metadata(table='Parts', out='tuples')])


#
# ========================================================================================================
# Test sqlite sqlalchemy client in memory
# ========================================================================================================
#
client = ConnectionPool(db_client='SQLAlchemy', dialect='sqlite', trace=3)


# initialize data of lists.
data = {'name': ['Tom', 'nick', 'krish', 'jack'],
        'age': [20, 21, 19, 18]}

# Create DataFrame
df = pd.DataFrame(data)

# Write dataframe into a database table
df.to_sql('Persons', client.connector.engine)

# Fetch all records from database table use the default output = `dataframe`
client.sql('SELECT * FROM Persons')
client.sql('SELECT * FROM Persons', out='tuples')
client.sql('SELECT name FROM Persons')

# Test DBAPIError exception
client.sql('SCT name FOM Persons')
