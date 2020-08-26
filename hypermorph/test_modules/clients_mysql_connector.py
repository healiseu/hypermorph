"""
HyperMorph Modules Testing:
    testing SQL queries with python MySQL-Connector
(C) July 2020 By Athanassios I. Hatzis
"""
from hypermorph.clients import ConnectionPool

# Test MySQL Client
cnx = ConnectionPool(db_client='MYSQL-Connector', host='localhost', port=3306,
                     user='demo', password='demo', database='SPC', trace=3)

# Select all columns from a database table
cnx.sql('SELECT * FROM Parts')
cnx.sql('SELECT * FROM Parts', out='tuples')
cnx.sql('SELECT * FROM Parts', out='named_tuples')
cnx.sql('SELECT * FROM Parts', out='json_rows')
cnx.sql('SELECT * FROM Parts', out='raw')
cnx.sql("SELECT * FROM Parts WHERE pcolor='Red'", out='dataframe', index='pid, pname')

# Select all columns from a database table with a WHERE condition
cnx.sql("SELECT * FROM Parts WHERE pcolor='Red'")
cnx.sql("SELECT * FROM Parts WHERE pcolor='Red'",
        as_columns='part_id, part_name, part_color, part_weight, part_unit')

# Select specific columns from a database table
cnx.sql('SELECT pid, pweight, punit FROM Parts')
cnx.sql('SELECT pid, pweight, punit FROM Parts', as_columns='part_id, part_weight, part_unit')
cnx.sql('SELECT pid, pweight, punit FROM Parts', as_columns='part_id, part_weight, part_unit', index='part_id')

# Select specific columns from a database table with a WHERE condition
cnx.sql("SELECT pid, pweight, punit FROM Parts WHERE punit='lb'")
cnx.sql("SELECT pid, pweight, punit FROM Parts WHERE punit='lb'", index='pid')

#
# PyArrow Transformations and Dictionary Encoding
#
# PyArrow Tables
cnx.sql('select * from Catalog', out='table')
cnx.sql('select catsid, catpid, catcost, catdate from Catalog', out='table')
cnx.sql('select catsid, catpid, catcost, catdate from Catalog', out='table', arrow_encoding=False)
cnx.sql('select catsid, catpid, catcost, catdate from Catalog', out='table', as_columns='sid, pid, price, date')
cnx.sql('select catsid, catpid, catcost, catdate from Catalog', out='table', as_columns=['sid', 'pid', 'price', 'date'])

# PyArrow RecordBatch
cnx.sql('select * from Catalog', out='batch', arrow_encoding=False)
cnx.sql('select * from Catalog', out='batch', arrow_encoding=True)

# Schema information for tables of a database with all columns
cnx.sql("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'Northwind'")

# Schema information for tables of a database with specific columns
q6 = '''
SELECT TABLE_NAME, TABLE_TYPE, ENGINE, VERSION, TABLE_ROWS, AVG_ROW_LENGTH, DATA_LENGTH, CREATE_TIME, TABLE_COLLATION 
FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'Northwind'
'''
cnx.sql(q6)

# Schema information for all tables in a database including schema tables
cnx.sql('SELECT * FROM INFORMATION_SCHEMA.COLUMNS')

# Test get_tables_metadata
cnx.get_tables_metadata()
cnx.get_tables_metadata(fields='TABLE_NAME, TABLE_ROWS, DATA_LENGTH', execute=False)
cnx.get_tables_metadata(fields='TABLE_NAME, TABLE_ROWS, DATA_LENGTH', index='TABLE_NAME')
table_names = [tbl[0] for tbl in cnx.get_tables_metadata(fields='TABLE_NAME', out='tuples')]
print(table_names)

# Test get_columns_metadata
cnx.get_columns_metadata()
cnx.get_columns_metadata(fields='COLUMN_NAME, COLUMN_TYPE, COLUMN_KEY', index='COLUMN_NAME')
cnx.get_columns_metadata(table='Orders')
cnx.get_columns_metadata(table='Orders', fields='COLUMN_NAME, DATA_TYPE', out='tuples')
