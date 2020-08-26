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
from sqltap import ProfilingSession
from sqlalchemy import create_engine
from sqlalchemy.exc import DBAPIError

from .exceptions import DBConnectionFailed, InvalidSQLOperation
from .utils import split_comma_string
import pandas as pd
import pyarrow as pa


# **********************************************************************
#   ******************** Class Specifications *********************
# **********************************************************************
class SQLAlchemy(object):
    def __init__(self, dialect=None, host=None, port=None, user=None, password=None, database=None, path=None, trace=0):

        self._dialect = dialect
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._path = path  # full path of SQLite database file
        self._trace = trace

        self._dburl = None
        self._con = None
        self._cursor = None
        self._q = None
        self._lqs = None
        self._profiler = ProfilingSession()

        try:
            # Construct database URL
            if dialect is None:
                self._dburl = 'sqlite:///:memory:'
            elif dialect == 'pymysql':  # pymysql Pure Python MySQL Client - (https://pymysql.readthedocs.io/)
                self._dburl = 'mysql+pymysql' + \
                              f'://{self._user}:{self._password}@{self._host}:{self._port}/{self._database}'
            elif dialect == 'clickhouse':
                # patch for sqlalchemy-clickhouse
                # https: // github.com / cloudflare / sqlalchemy - clickhouse / issues / 49
                self._dburl = f'clickhouse' + \
                              f'://{self._user}:{self._password}@{self._host}:{self._port}/{self._database}'
            elif dialect == 'sqlite':
                self._dburl = f'sqlite:///{self._path}'

            # Create sqlalchemy engine
            self._engine = create_engine(self._dburl)
            # Create database connection
            self._con = self._engine.connect()
            self._cursor = self._con.connection.cursor()
        except Exception:
            raise DBConnectionFailed(f'SQLAlchemy operation to create engine failed')

    @property
    def engine(self):
        return self._engine

    @property
    def cursor(self):
        return self._cursor

    @property
    def connection(self):
        return self._con

    @property
    def api_category(self):
        if self._dialect == 'pymysql':
            return 'MYSQL'
        elif self._dialect == 'clickhouse':
            return 'CLICKHOUSE'
        elif self._dialect == 'sqlite':
            return 'SQLite'

    @property
    def last_query(self):
        return self._q

    @property
    def last_query_stats(self):
        return self._lqs

    @staticmethod
    def _rows_to_table(cursor_data, arrow_table, arrow_encoding):
        rec_batch = pa.RecordBatch.from_pandas(cursor_data)
        # dictionary encoding
        if arrow_encoding:
            columns_dict_encoded = [col.dictionary_encode() for col in rec_batch]
            rec_batch = pa.RecordBatch.from_arrays(columns_dict_encoded, names=cursor_data.columns)

        # PyArrow Table transformation
        if arrow_table:
            result = pa.Table.from_batches([rec_batch])
        else:
            result = rec_batch

        return result

    def sql(self, sql, out='dataframe', execute=True, trace=None, arrow_encoding=True,
            as_columns=None, index=None, partition_size=None, **kwargs):
        """
        :param sql:  sql query string that will be send to server
        :param out: output format e.g. dataframe, tuples, ....
        :param execute: flag to enable execution of SQL statement
        :param trace: trace execution of query, i.e. print query, ellapsed time, rows in set,  etc....
        :param partition_size: number of records to use for each partition or target size of each partition, in bytes
        :param arrow_encoding: PyArrow columnar dictionary encoding
        :param as_columns: user specified column names for pandas dataframe
                           (list of strings, or comma separated string)
        :param index: column names to be used in pandas dataframe index
        :param kwargs: parameters passed to pandas.read_sql() method
                https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_sql.html
        :return: result formatted according to the `out` parameter
        """

        # Initialization stage
        result = None

        if trace:
            self._trace = trace

        # Try to execute sql query on the database server
        if execute:
            try:
                if out in ['dataframe', 'table', 'batch']:
                    # Construct the dataframe first
                    if partition_size:
                        kwargs['chunksize'] = partition_size
                    if index:
                        kwargs['index_col'] = split_comma_string(index)
                    # Use pandas.read_sql method
                    with self._profiler:
                        result = pd.read_sql(sql=sql, con=self._con, **kwargs)
                        if as_columns:
                            result.columns = split_comma_string(as_columns)
                    # Construct the PyArrow RecordBatch
                    if out == 'batch':
                        result = self._rows_to_table(result, arrow_table=False, arrow_encoding=arrow_encoding)
                    elif out == 'table':
                        result = self._rows_to_table(result, arrow_table=True, arrow_encoding=arrow_encoding)
                elif out == 'tuples':
                    # Use cursor method of the API
                    with self._profiler:
                        self._cursor.execute(sql)
                        result = self._cursor.fetchall()
                elif out == 'json_rows':
                    with self._profiler:
                        if as_columns:
                            as_columns = split_comma_string(as_columns)
                            self._cursor.execute(sql)
                            result = [dict(zip(as_columns, rec)) for rec in self._cursor.fetchall()]
                        else:
                            result = [{column: value for column, value in rowproxy.items()}
                                      for rowproxy in self.connection.execute(sql)]
                else:
                    raise InvalidSQLOperation(f'SQL execution failed, unknown output format specified: {out}')

                profiler_info = self._profiler.collect()
                if profiler_info:
                    self._lqs = profiler_info.pop()
            except DBAPIError as err:
                print(err)
                return None

        # Store last SQL query string
        self._q = sql

        # Debug info
        if self._trace > 1:
            print(f'{self._q}\n╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌')
        if execute and self._trace > 0:
            print(f'Elapsed: {round(self._lqs.duration, 3)} sec, {self._lqs.rowcount} rows in set.\n',
                  '\n___________________________________________________________________________')

        if result is None:
            if self._trace > 0:
                print('Done.\n╚═══════════════════════════════════════════════════════════════════════╝')

        return result
# ***************************************************************************************
# ************************** End of SQLAlchemy Class **************************************
# ***************************************************************************************
