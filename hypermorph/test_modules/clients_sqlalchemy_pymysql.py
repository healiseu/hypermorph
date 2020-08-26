"""
HyperMorph Modules Testing:
    testing mysql queries with pymysql and SQLAlchemy python clients
(C) August 2020 By Athanassios I. Hatzis
"""
from hypermorph.clients import ConnectionPool

# Test pymysql (https://pymysql.readthedocs.io/)
client = ConnectionPool(db_client='SQLAlchemy', dialect='pymysql',
                        host='localhost', port=3306, user='demo', password='demo', database='SPC', trace=3)
print(client)
print(client.api_name)
print(client.api_category)
print(client.database)
print(client.sqlalchemy_dialect)
print(client.connector)
print(client.connector.cursor)
print(client.connector.connection)
print(client.connector.engine)
print(client.connector.last_query)
print(client.connector.last_query_stats)


nw = ConnectionPool(db_client='SQLAlchemy', dialect='pymysql',
                    host='localhost', port=3306, user='demo', password='demo', database='Northwind', trace=3)

# Test sql() method on a table's data
client.sql('SELECT * FROM Parts')
client.sql('SELECT * FROM Parts', out='tuples')
client.sql('SELECT * FROM Parts', out='json_rows')
client.sql('SELECT pid, pname, pweight FROM Parts')
client.sql('SELECT pid, pname, pweight, punit FROM Parts', index='pid, pname')
client.sql('SELECT pid, pname, pweight, punit FROM Parts', index=['pid', 'pname'])
client.sql("SELECT pid, pweight, punit FROM Parts WHERE punit='lb'")
client.sql('SELECT * FROM Parts', execute=False)

# Test sql() exceptions
client.sql('SELECT name FOM Persons')
client.sql('SELECT pid, pname, pweight FROM Persons')

# Test sql() on MySQL schema information
client.sql("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'Northwind'")
client.sql('''
SELECT TABLE_NAME, TABLE_TYPE, ENGINE, VERSION, TABLE_ROWS, AVG_ROW_LENGTH, DATA_LENGTH, CREATE_TIME, TABLE_COLLATION 
FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'Northwind'
''')
client.sql("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'Northwind'")
client.sql('''
SELECT TABLE_NAME, COLUMN_NAME, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, COLUMN_TYPE, COLUMN_KEY, COLLATION_NAME
FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'Northwind'
''')

# Test get_tables_metadata
nw.get_tables_metadata()
nw.get_tables_metadata(fields='TABLE_NAME, TABLE_ROWS, DATA_LENGTH', execute=False)
nw.get_tables_metadata(fields='TABLE_NAME, TABLE_ROWS, DATA_LENGTH', index='TABLE_NAME')
table_names = [tbl[0] for tbl in nw.get_tables_metadata(fields='TABLE_NAME', out='tuples')]
print(table_names)

# Test get_columns_metadata
nw.get_columns_metadata()
nw.get_columns_metadata(fields='COLUMN_NAME, COLUMN_TYPE, COLUMN_KEY', index='COLUMN_NAME')
nw.get_columns_metadata(table='Orders')
nw.get_columns_metadata(table='Orders', fields='COLUMN_NAME, DATA_TYPE', out='tuples')
