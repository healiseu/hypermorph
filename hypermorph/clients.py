"""
This file is part of HyperMorph operational API for information management and data transformations
on Associative Semiotic Hypergraph Development Framework
(C) 2015-2020 Athanassios I. Hatzis

HyperMorph is free software: you can redistribute it and/or modify it under the terms of
the GNU Affero General Public License v.3.0 as published by the Free Software Foundation.

HyperMorph is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with HyperMorph.
If not, see <https://www.gnu.org/licenses/>.
"""
import pandas as pd

from . import db_types, db_clients, sqlalchemy_dialects
from .exceptions import DBConnectionFailed, InvalidSQLOperation
from .utils import sql_construct

# These are the client APIs that have been integrated in HyperMorph
# Functionality is extended with a uniform interface to run sql commands and methods to access database metadata
from .connector_clickhouse_driver import ClickHouse
from .connector_sqlalchemy import SQLAlchemy
from .connector_mysql import MySQL


class ConnectionPool(object):
    """
    ConnectionPool manages connections to DBMS and extends API database clients
    i) with useful introspective properties (api, database, sqlalchemy_engine, last_query, query_stats)
    ii) with a uniform sql command interface (sql method)
    iii) with common methods to access database metadata  (get_tables_metadata, get_columns_metadata)

    HyperMorph currently supports the following three database API clients (self._api_name)
        A) Clickhouse-Driver
        B) MySQL-Connector
        C) SQLAlchemy with the following three dialects (self._sqlalchemy_dialect)
            1) pymysql
            2) clickhouse
            3) sqlite

    Consequently various database APIs are categorized as (self._api_category)
        MYSQL
        CLICKHOUSE
        SQLite
    """

    mysql_connections = 0
    clickhouse_connections = 0
    sqlite_connections = 0

    def __init__(self, db_client, dialect=None,
                 host=None, port=None, user=None, password=None, database=None, path=None, trace=0):
        """
        :param db_client: API name of the database client, i.e. python library
        :param dialect: SQLAlchemy dialect
        :param host:
        :param port:
        :param user:
        :param password:
        :param database:
        :param path:
        :param trace:
        """

        # Check db_client
        if db_client not in db_clients:
            raise DBConnectionFailed(f'Connection to DBMS failed <{db_client} is not supported>')

        # Check sqlalchemy dialect
        if dialect is not None and dialect not in sqlalchemy_dialects:
            raise DBConnectionFailed(f'Connection to DBMS failed <sqlalchemy dialect: {dialect} is not supported>')

        # Get connector class
        connector_class = self._get_connector(db_client, dialect)

        # Create a new connector, i.e. object that represents a database API client
        if db_client == 'SQLAlchemy':
            self._connector = connector_class(dialect, host, port, user, password, database, path, trace)
        else:
            self._connector = connector_class(host, port, user, password, database, trace)

        self._trace = trace     # flag to display more information during execution of query
        self._host = host       # host connection parameter, name or IP address
        self._port = port       # the port number used by the database server
        self._user = user
        self._password = password
        self._api_name = db_client   # the name of the database API client
        self._sqlalchemy_dialect = dialect
        self._database = database

        # Check database API category
        self._api_category = self._connector.api_category
        if self._api_category not in db_types:
            raise DBConnectionFailed(f'Connection to DBMS failed. Connector returned unknown DB API category')

        if self._trace > 3:
            print(f'\nConnected to {self.__repr__()}')

    @staticmethod
    def _get_connector(db_client, dialect=None):
        if db_client == 'Clickhouse-Driver':
            ConnectionPool.clickhouse_connections += 1
            return ClickHouse
        elif db_client == 'MYSQL-Connector':
            ConnectionPool.mysql_connections += 1
            return MySQL
        elif db_client == 'SQLAlchemy':
            if dialect == 'pymysql':
                ConnectionPool.mysql_connections += 1
            elif dialect == 'clickhouse':
                ConnectionPool.clickhouse_connections += 1
            elif dialect == 'sqlite':
                ConnectionPool.sqlite_connections += 1
            return SQLAlchemy
        else:
            raise DBConnectionFailed(f'Connection to DBMS failed <{db_client} is not supported>')

    @property
    def api_name(self):
        return self._api_name

    @property
    def api_category(self):
        return self._api_category

    @property
    def sqlalchemy_dialect(self):
        return self._sqlalchemy_dialect

    @property
    def database(self):
        return self._database

    @property
    def connector(self):
        return self._connector

    def sql(self, query, **kwargs):
        """
        For kwargs and specific details on implementation see implementation of connector class for the specific API
        e.g. SQLAlchemy.sql() for sqlalchemy database API
        :param query:
        :param kwargs: pass other parameters to sql() method of connector class
        :return: result set represented with a pandas dataframe, tuples, ....
        """
        return self._connector.sql(query, **kwargs)

    def _get_mysql_tables_query(self, flds=None):
        if not flds:
            flds = 'TABLE_NAME, TABLE_TYPE, ENGINE, VERSION, ' \
                   'TABLE_ROWS, AVG_ROW_LENGTH, DATA_LENGTH, CREATE_TIME, TABLE_COLLATION'
        sel = f'SELECT {flds} '
        frm = f'FROM INFORMATION_SCHEMA.TABLES'
        wh = f"WHERE TABLE_SCHEMA = '{self._database}'"
        query = sql_construct(select=sel, frm=frm, where=wh)
        return query

    @staticmethod
    def _get_sqlite_tables_query(flds=None):
        if not flds:
            flds = 'name, tbl_name, sql'
        sel = f'SELECT {flds} '
        frm = f'FROM sqlite_master'
        wh = f"WHERE type='table'"
        query = sql_construct(select=sel, frm=frm, where=wh)
        return query

    def _get_clickhouse_tables_query(self, flds=None, eng=None, name=None):
        if not flds:
            flds = 'database, engine, name, partition_key, sorting_key, primary_key'
        sel = f'SELECT {flds} '
        frm = f'FROM system.tables'
        wh = f"WHERE database='{self._database}'"
        if name:
            wh += f' AND name like \'%{name}%\''
        if eng:
            wh += f" AND engine='{eng}'"
        order = f'ORDER BY database, engine'
        # construct query
        query = sql_construct(select=sel, frm=frm, where=wh, order=order)
        return query

    @staticmethod
    def _get_sqlite_columns_query(tbl):
        query = f'PRAGMA table_info({tbl})'
        return query

    def _get_mysql_columns_query(self, tbl=None, flds=None):
        if not flds:
            flds = 'TABLE_NAME, COLUMN_NAME, COLUMN_DEFAULT, ' \
                   'IS_NULLABLE, DATA_TYPE, COLUMN_TYPE, COLUMN_KEY, COLLATION_NAME'
        sel = f'SELECT {flds} '
        frm = f'FROM INFORMATION_SCHEMA.COLUMNS'
        wh = f"WHERE TABLE_SCHEMA='{self._database}'"
        if tbl:
            wh += f"AND TABLE_NAME='{tbl}'"
        query = sql_construct(select=sel, frm=frm, where=wh)
        return query

    def get_columns_metadata(self, table=None, columns=None, fields=None, aggr=None, **kwargs):
        """
        :param table: name of the table in database
        :param columns: list of ClickHouse column names
        :param fields: select specific meta-data fields for the columns of a table in database dictionary
                metadata field names are dependent on the specific DBMS e.g. MySQL, SQLite, ClickHouse, etc...
        :param aggr: aggregate metadata results for the columns of a clickhouse table
        :param kwargs: pass extra parameters to sql() method
        :return: metadata for the columns of a table(s) in a database e.g. name of column, default value, nullable, etc
        """
        if self._api_category == 'MYSQL':
            query = self._get_mysql_columns_query(table, fields)
        elif self._api_category == 'CLICKHOUSE':
            if self._sqlalchemy_dialect:
                order = f'ORDER BY type DESC'
                if not fields:
                    fields = 'name, comment, type, ' \
                             'data_compressed_bytes/1048576 as Compressed_MB, ' \
                             'data_uncompressed_bytes/1048576 as Uncompressed_MB, ' \
                             'marks_bytes/1024 as marks_KB'
                sel = f'SELECT {fields} '
                frm = f'FROM system.columns'
                wh = f"WHERE database='{self._database}'"
                if table:
                    wh += f"AND table='{table}'"
                query = sql_construct(select=sel, frm=frm, where=wh, order=order)
            else:   # use clickhouse-driver
                # execute=False because we want to construct the sql query
                query = self._connector.get_columns(table, columns, fields, aggr, execute=False, **kwargs)
        elif self._api_category == 'SQLite':
            if table and fields:
                query = self._get_sqlite_columns_query(table)
                result = self.sql(query)[fields.split(', ')]
                if 'out' in kwargs and kwargs['out'] == 'tuples':
                    result = result.values
                return result
            elif table:
                query = self._get_sqlite_columns_query(table)
            else:
                # This is a special case for sqlite database to return all columns for all tables in database
                q = self._get_sqlite_tables_query('name')
                table_names = [tbl[0] for tbl in self.sql(q, out='tuples')]
                dflist = []
                for tblnam in table_names:
                    q = self._get_sqlite_columns_query(tblnam)
                    df = self.sql(q)
                    df['table'] = tblnam
                    dflist.append(df)
                result = pd.concat(dflist)
                if 'out' in kwargs and kwargs['out'] == 'tuples':
                    return result.values
                else:
                    return result
        else:
            raise InvalidSQLOperation(f'Invalid SQL operation, cannot handle DB API category {self._api_category}')

        return self.sql(query, **kwargs)

    def get_tables_metadata(self, fields=None, clickhouse_engine=None, name=None, **kwargs):
        """
        :param clickhouse_engine: type of storage engine for clickhouse database
        :param fields: select specific meta-data fields for a table in database dictionary
                metadata field names are dependent on the specific DBMS e.g. MySQL, SQLite, ClickHouse, etc...
        :param name: table name regular expression e.g. name='%200%'
        :param kwargs: parameters passed to sql() method
        :return: metadata for the tables of a database e.g. name of table, number of rows, row length, collation, etc..
        """
        if self._api_category == 'MYSQL':
            query = self._get_mysql_tables_query(fields)
        elif self._api_category == 'SQLite':
            query = self._get_sqlite_tables_query(fields)
        elif self._api_category == 'CLICKHOUSE':
            query = self._get_clickhouse_tables_query(fields, clickhouse_engine, name)
        else:
            raise InvalidSQLOperation(f'Invalid SQL operation, cannot handle DB API category {self._api_category}')

        return self.sql(query, **kwargs)

    def __repr__(self):
        if self._api_name == 'SQLAlchemy':
            return f'{self._api_name}(host = {self._host}, port = {self._port}, ' \
                   f'database = {self._database}, sqlalchemy_engine = {self._connector.engine})'
        else:
            return f'{self._api_name}(host = {self._host}, port = {self._port}, database = {self._database}'

# ***************************************************************************************
# ************************** End of ConnectionPool Class ***********************
# ***************************************************************************************


# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
# ***************************************************************************************
#                   =======   End of HyperMorph_clients Module =======
# ***************************************************************************************
# \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
