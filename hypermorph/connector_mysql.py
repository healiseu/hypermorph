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
import pyarrow as pa
import mysql
from mysql.connector import errorcode, FieldType
from .exceptions import DBConnectionFailed, PandasError, InvalidSQLOperation
from .utils import PandasUtils, DSUtils, split_comma_string

# DataType Conversion MySQL Connector FieldType module data types  ---> PyArrow
mysql_to_pyarrow_dtype = {
    '1': 'bool',

    'BIT': 'uint8',
    '3': 'uint16',
    '4': 'uint32',
    'LONGLONG': 'uint64',

    'TINY': 'int8',
    'SHORT': 'int16',
    'LONG': 'int32',

    'DECIMAL': 'decimal128',
    'NEWDECIMAL': 'decimal128',
    'FLOAT': 'float32',
    'DOUBLE': 'float64',

    'DATE': 'date32',
    'TIMESTAMP': 'timestamp',

    'VARCHAR': 'string',
    'ENUM': 'string',
    'VAR_STRING': 'string',
    'STRING': 'string',
    'TINY_BLOB': 'string',
    'MEDIUM_BLOB': 'string',
    'LONG_BLOB': 'string',
    'BLOB': 'string'
}


class MySQL(object):
    def __init__(self, host, port, user, password, database, trace=0):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._trace = trace

        self._con = None
        self._cursor = None
        self._api = None
        self._api_category = 'MYSQL'
        self._q = None

        try:
            self._con = mysql.connector.connect(host=self._host, port=self._port,
                                                user=self._user, password=self._password, database=self._database)
            self._cursor = self._con.cursor(buffered=True)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Access denied, wrong user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
                raise DBConnectionFailed(f'Connection to MariaDB failed. Check connection parameters')

    @property
    def cursor(self):
        return self._cursor

    @property
    def connection(self):
        return self._con

    @property
    def api_category(self):
        return 'MYSQL'

    @property
    def last_query(self):
        return self._q

    def set_cursor(self, buffered=True, raw=None, dictionary=None, named_tuple=None):
        return self._con.cursor(buffered=buffered, raw=raw, dictionary=dictionary, named_tuple=named_tuple)

    def _rows_to_table(self, cursor_data, as_columns, arrow_table, arrow_encoding):
        arrow_struct_field_types = []
        for desc in self._cursor.description:
            mysql_dtype = FieldType.get_info(desc[1])
            arrow_dtype_string = mysql_to_pyarrow_dtype[mysql_dtype]
            # PyArrow Issue: It does not support struct_arrays with dictionary encoded fields
            # https://issues.apache.org/jira/browse/ARROW-9505
            # https://issues.apache.org/jira/browse/ARROW-3978
            arrow_dtype = DSUtils.pyarrow_dtype_from_string(arrow_dtype_string, dictionary=False)
            arrow_struct_field_types.append(arrow_dtype)
        struct_schema = pa.struct(list(zip(as_columns, arrow_struct_field_types)))
        # RecordBatch but without dictionary encoding see issue above
        rec_batch = pa.RecordBatch.from_struct_array(pa.array(cursor_data, type=struct_schema))

        # dictionary encoding
        if arrow_encoding:
            columns_dict_encoded = [col.dictionary_encode() for col in rec_batch]
            # RecordBatch with dictionary encoding, it works but we create a copy
            rec_batch = pa.RecordBatch.from_arrays(columns_dict_encoded, names=as_columns)  # ignore inspection
        # PyArrow RecordBatch or PyArrow Table
        if arrow_table:
            result = pa.Table.from_batches([rec_batch])
        else:
            result = rec_batch

        return result

    def sql(self, sql, out='dataframe', as_columns=None, index=None,  partition_size=None,
            arrow_encoding=True, execute=True, buffered=True, trace=None):
        """
        This method is calling the cursor.execute() method of mysql.connector to execute sql query
        Connection has already been established.
        :param sql: mysql query string that will be send to server
        :param out: output format, i.e. python data structure that will represent the result set
                    dataframe, tuples, named_tuples, json_rows
        :param partition_size:
               ToDo number of records to use for each partition or target size of each partition, in bytes
        :param arrow_encoding: PyArrow columnar dictionary encoding
        :param as_columns: user specified column names for pandas dataframe
                           (list of strings, or comma separated string)
        :param index: column names to be used in pandas dataframe index
        :param execute: execute SQL commands only if execute=True
        :param trace: trace execution of query, i.e. print query, ellapsed time, rows in set,  etc....
        :param buffered:
            MySQLCursorBuffered cursor fetches the entire result set from the server and buffers the rows.
            For nonbuffered cursors, rows are not fetched from the server until a row-fetching method is called.

        :return: result set formatted according to the `out` parameter

        For more details about MySQLCursor class execution see
        https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlcursor.html
        """
        # Initialization stage
        result = None
        if trace:
            self._trace = trace

        # Prepare cursor
        if out in ['dataframe', 'tuples', 'batch', 'table']:
            self._cursor = self.set_cursor(dictionary=None, named_tuple=None, raw=None, buffered=buffered)
        elif out == 'named_tuples':
            self._cursor = self.set_cursor(dictionary=None, named_tuple=True, raw=None, buffered=buffered)
        elif out == 'json_rows':
            self._cursor = self.set_cursor(dictionary=True, named_tuple=None, raw=None, buffered=buffered)
        elif out == 'raw':
            # skips the conversion from MySQL data types to Python types when fetching rows.
            # A raw cursor is usually used to get better performance or when you want to do the conversion yourself.
            # returns Python bytearray objects
            self._cursor = self.set_cursor(dictionary=None, named_tuple=None, raw=True, buffered=buffered)
        else:
            raise InvalidSQLOperation(f'SQL execution failed, unknown output format specified: {out}')

        # Try to execute sql query on the database server
        if execute:
            try:
                self._cursor.execute(sql)
                result = self._cursor.fetchall()
            except mysql.connector.Error as err:
                print(err)
                return None

        if as_columns is None:
            # Change the projection of the query with those column names specified by the user
            as_columns = self._cursor.column_names
        else:
            as_columns = split_comma_string(as_columns)

        # Convert result set to Pandas DataFrame or PyArrow Table or PyArrow RecordBatch
        try:
            if out == 'dataframe':
                result = PandasUtils.dataframe(result, as_columns, index)
            elif out == 'batch':
                result = self._rows_to_table(result, as_columns, arrow_table=False, arrow_encoding=arrow_encoding)
            elif out == 'table':
                result = self._rows_to_table(result, as_columns, arrow_table=True, arrow_encoding=arrow_encoding)
        except Exception:
            print(sql)
            raise InvalidSQLOperation(f'Failed to construct Pandas DataFrame or PyArrow Table')

        # Store last SQL query string
        self._q = sql
        # Debug info
        if self._trace > 1:
            print(f'{self._q}\n╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌')

        return result

    def close(self):
        self._con.close()

# ***************************************************************************************
# ************************** End of MySQL Class **************************************
# ***************************************************************************************
