"""
HyperMorph Modules Testing:
    creating instance of hypermorph.clients.ConnectionPool
    testing connection to database management systems (ClickHouse, MariaDB)

(C) October 2019 By Athanassios I. Hatzis
"""
from hypermorph.clients import ConnectionPool

#
# Test Connections
#

# Test mymarilin/clickhouse-driver (https://github.com/mymarilyn/clickhouse-driver)
ch = ConnectionPool(db_client='Clickhouse-Driver', host='localhost', port=9000,
                    user='demo', password='demo', database='TestDB', trace=4)
ch.sql('SHOW TABLES', as_columns='Tables')

# Test MySQL Client
cnx = ConnectionPool(db_client='MYSQL-Connector', host='localhost', port=3306,
                     user='demo', password='demo', database='SPC', trace=3)
cnx.sql('SHOW TABLES', as_columns='Tables')

#
# =======================================================================
# SQLAlchemy Connections
# =======================================================================
#
# Test sqlite sqlalchemy client
sqlite = ConnectionPool(db_client='SQLAlchemy', dialect='sqlite', path='/data/DataSQLite/spc.db')
sqlite.sql('SELECT * FROM Parts')

# Test pymysql sqlalchemy client (https://pymysql.readthedocs.io/)
mysql = ConnectionPool(db_client='SQLAlchemy', dialect='pymysql',
                       host='localhost', port=3306, user='demo', password='demo', database='SPC', trace=3)
mysql.sql('SELECT * FROM Parts')

# Test clickhouse sqlalchemy client
clickhouse = ConnectionPool(db_client='SQLAlchemy', dialect='clickhouse',
                            host='localhost', port=8123, user='demo', password='demo', database='TriaDB', trace=3)
clickhouse.sql('SELECT * FROM DAT_242_1')
