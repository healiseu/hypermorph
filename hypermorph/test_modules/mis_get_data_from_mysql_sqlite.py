"""
HyperMorph Modules Testing:
    Fetch data from two database tables, one table from a MySQL database, another table from a SQLite database
    Tables have similar data types and data and we use the same method chaining for both of them
(C) August 2020 By Athanassios I. Hatzis
"""
from hypermorph import MIS
from hypermorph.clients import ConnectionPool

# Load Schema
mis = MIS(debug=1, load=True, net_name='my_schema_binary', net_format='gt', net_path='/data/test/Schemata')

order_fields = 'OrderID, EmployeeID, OrderDate, Freight, ShipAddress'
col_names = 'OID, EID, ODate, OFreight, SAddress'

#
# Northwind MYSQL Database - OrderDetails Table
#
mis.get(135).tables
mis.get(143).get_rows().over().to_dataframe().out()
mis.get(143).get_rows().over('OrderID, ProductID, Quantity').slice(10).to_dataframe(index='OrderID, ProductID').out()
mis.get(143).get_rows().over('OrderID, ProductID, Quantity').slice(10).to_tuples().out()

# Arrow transformations
mis.get(139).get_rows().over(order_fields).slice(10).to_table().out()
mis.get(139).get_rows().over(select=order_fields, as_names=col_names).slice(10).to_table().out()
mis.get(139).get_rows().over(select=['OrderID', 'EmployeeID'], as_names=['OID', 'EID']).slice(10).to_table().out()
mis.get(139).get_rows().over(order_fields).slice(10).to_batch().out()
mis.get(139).get_columns().over(order_fields).slice(10).to_batch(arrow_encoding=True).out()
mis.get(139).get_columns().over(order_fields).slice(10).to_batch(arrow_encoding=False).out()


#
# Northwind SQLite Database - OrderDetails Table
#
mis.get(26).tables
mis.get(31).get_rows().over().to_dataframe().out()
mis.get(31).get_rows().over('OrderID, ProductID, Quantity').slice(10).to_dataframe(index='OrderID, ProductID').out()
mis.get(31).get_rows().over('OrderID, ProductID, Quantity').slice(10).to_tuples().out()

# Arrow transformations
mis.get(32).get_rows().over(order_fields).slice(10).to_table().out()
mis.get(32).get_rows().over(select=order_fields, as_names=col_names).slice(10).to_table().out()
mis.get(32).get_rows().over(select=['OrderID', 'EmployeeID'], as_names=['OID', 'EID']).slice(10).to_table().out()
mis.get(32).get_rows().over(order_fields).slice(10).to_batch().out()
mis.get(32).get_columns().over(order_fields).slice(10).to_batch(arrow_encoding=True).out()
mis.get(32).get_columns().over(order_fields).slice(10).to_batch(arrow_encoding=False).out()

# Change SQLlite connection to read another database file
sqlite = ConnectionPool(db_client='SQLAlchemy', dialect='sqlite', path='/data/DataSQLite/spc.db')
sqlite.sql('SELECT * FROM Parts', out='tuples')
sqlite.sql('SELECT * FROM Parts', out='json_rows')
