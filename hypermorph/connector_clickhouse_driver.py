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
import time
import pyarrow as pa
from clickhouse_driver import Client

from .exceptions import DBConnectionFailed, InvalidSQLOperation, InvalidEngine, PandasError
from .utils import PandasUtils, sql_construct, split_comma_string

engine_types = ['MergeTree', 'ReplacingMergeTree', 'Memory']

# DataType Conversion ClickHouse ---> PyArrow
clickhouse_to_pyarrow_dtype = {
    'UInt8Boolean': 'bool',
    'UInt8': 'uint8',
    'Nullable(UInt8)': 'uint8',

    'UInt16': 'uint16',
    'Nullable(UInt16)': 'uint16',

    'UInt32': 'uint32',
    'Nullable(UInt32)': 'uint32',

    'UInt64': 'uint64',
    'Nullable(UInt64)': 'uint64',

    'Int8': 'int8',
    'Nullable(Int8)': 'int8',

    'Int16': 'int16',
    'Nullable(Int16)': 'int16',

    'Int32': 'int32',
    'Nullable(Int32)': 'int32',

    'Int64': 'int64',
    'Nullable(Int64)': 'int64',

    'Float32': 'float32',
    'Nullable(Float32)': 'float32',

    'Float64': 'float64',
    'Nullable(Float64)': 'float64',

    'Date': 'date32',
    'Nullable(Date)': 'date32',

    'DateTime': 'timestamp',

    'LowCardinality(Nullable(String))': 'string',
    'Nullable(String)': 'string',
    'String': 'string'
}


class ClickHouse(object):
    """
    ClickHouse class is based on clickhouse-driver python API for ClickHouse DBMS
    """

    def __init__(self, host, port, user, password, database, trace=0):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._trace = trace

        self._api_category = 'CLICKHOUSE'
        self._client = None  # This is used in the place of a cursor other APIs, e.g. sqlalchemy, have

        # Query execution statistics
        self._q = None  # Query
        self._qid = None  # Query ID
        self._rset_rows = None  # Rows in the result set before applying limit
        self._rset_total = None  # Total number of rows in the result set
        self._processed_rows = None  # Rows processed to create the result set
        self._processed_bytes = None  # Volume of data processed to create the result set
        self._elapsed_time = None  # Time elapsed to return the result

        if host == 'localhost':
            self._host = '127.0.0.1'
        # Create connection using the pure Client
        try:
            self._client = Client(host=host, database=database, port=port, user=user, password=password)
        except Exception:
            raise DBConnectionFailed(f'Connection to ClickHouse failed. Check connection parameters')

    @property
    def cursor(self):
        return self._client

    @property
    def connection(self):
        return self._client.connection

    @property
    def api_category(self):
        return 'CLICKHOUSE'

    @property
    def last_query_statistics(self):
        return [self._q, self._qid, self._rset_rows, self._elapsed_time,
                self._processed_rows, self._processed_bytes, self._rset_total]

    @property
    def print_query_statistics(self):
        lqs = self.last_query_statistics
        print(f'{lqs[0]}\n--------------------------------------------------------')
        print(f'QueryID:{lqs[1]}\nElapsed: {round(lqs[3], 3)} sec',
              f'{lqs[2]} rows in set.\nProcessed: {lqs[4]} rows, {round(lqs[5] / 1048576, 3)} MB')
        return None

    @staticmethod
    def _column_to_pyarrow_array(data, dtype, dictionary_encoding):
        """
        :param data: values of the column in python list
        :param dtype: PyArrow data type
        :param dictionary_encoding: whether to use or not PyArrow columnar dictionary encoding
        :return: PyArrow Array or Dictionary Array when dictionary_encoding=True
        """
        if dictionary_encoding:
            val_arr = pa.array(data, type=dtype).dictionary_encode()
        else:
            val_arr = pa.array(data, type=dtype)
        return val_arr

    @staticmethod
    def _from_client_columns_to_arrow(as_columns, column_meta, column_data, arrow_table, arrow_encoding):
        """
        :param as_columns:
        :param column_meta:
        :param column_data:
        :param arrow_table:
        :param arrow_encoding:
        :return:
        """
        # If it is a single column transform to PyArrow Array or
        # to a Dictionary Array when dictionary_encoding=True
        if len(column_meta) == 1:
            clickhouse_dtype = column_meta[0][1]
            # Convert clickhouse data type to PyArrow data type
            arrow_dtype = getattr(pa, clickhouse_to_pyarrow_dtype[clickhouse_dtype])()
            column_values = column_data[0]
            result = ClickHouse._column_to_pyarrow_array(column_values, arrow_dtype, arrow_encoding)
        # Otherwise transform columns to a RecordBatch with optional dictionary encoding
        else:
            arrow_names = []
            arrow_arrays = []

            for column_pos, column_name_dtype in enumerate(column_meta):
                # Get column metadata
                column_name, column_dtype = column_name_dtype
                arrow_names.append(column_name)
                # Convert clickhouse data type to PyArrow data type
                arrow_dtype = getattr(pa, clickhouse_to_pyarrow_dtype[column_dtype])()
                column_values = column_data[column_pos]
                arrow_column = ClickHouse._column_to_pyarrow_array(column_values,
                                                                   arrow_dtype, arrow_encoding)
                arrow_arrays.append(arrow_column)

            if as_columns:
                arrow_names = as_columns

            if arrow_table:
                result = pa.Table.from_arrays(arrow_arrays, names=arrow_names)  # ignore inspection
            else:
                result = pa.RecordBatch.from_arrays(arrow_arrays, names=arrow_names)  # ignore inspection

        return result

    def sql(self, sql, out='dataframe', as_columns=None, index=None, partition_size=None, arrow_encoding=True,
            params=None, qid=None, execute=True, trace=None):
        """
        This method is calling clickhouse-driver execute() method to execute sql query
        Connection has already been established.
        :param sql: clickhouse SQL query string that will be send to server
        :param out: output format, i.e. python data structure that will represent the result set
                    dataframe, tuples, json_rows
        :param as_columns: user specified column names for pandas dataframe,
               (list of strings, or comma separated string)
        :param index: pandas dataframe columns
        :param arrow_encoding: PyArrow columnar dictionary encoding
        :param arrow_table: Output is PyArrow Table, otherwise it is a PyArrow RecordBatch
        :param partition_size:
               ToDo number of records to use for each partition or target size of each partition, in bytes
        :param params: clickhouse-client execute parameters
        :param qid: query identifier. If no query id specified ClickHouse server will generate it
        :param execute: execute SQL commands only if execute=True
        :param trace: trace execution of query, i.e. print query, ellapsed time, rows in set,  etc....

        :return: result set formatted according to the `out` parameter
        """
        # Initialization stage
        result, t_start, t_end = [None] * 3
        if trace:
            self._trace = trace

        self._q = sql
        self._qid = qid
        (self._elapsed_time, self._rset_rows, self._processed_rows,
         self._processed_bytes, self._rset_total) = [0, 0, 0, 0, 0]

        # clickhouse-driver execution of sql statement according to the output format
        # ToDO: numpy arrays from columnar output without copying
        # https://docs.scipy.org/doc/numpy/reference/generated/numpy.array.html
        if execute:
            if as_columns:
                as_columns = split_comma_string(as_columns)

            if out == 'tuples':
                result = self._client.execute(query=sql, params=params, query_id=qid)
            elif out == 'columns':
                result = self._client.execute(query=sql, params=params, columnar=True, query_id=qid)
            elif out == 'batch':
                # Fetch column data and types from ClickHouse
                column_data, column_meta = self._client.execute(query=sql, params=params, columnar=True,
                                                                with_column_types=True, query_id=qid)
                result = self._from_client_columns_to_arrow(as_columns, column_meta, column_data,
                                                            arrow_table=False, arrow_encoding=arrow_encoding)
            elif out == 'table':
                # Fetch column data and types from ClickHouse
                column_data, column_meta = self._client.execute(query=sql, params=params, columnar=True,
                                                                with_column_types=True, query_id=qid)
                result = self._from_client_columns_to_arrow(as_columns, column_meta, column_data,
                                                            arrow_table=True, arrow_encoding=arrow_encoding)
            elif out in ['dataframe', 'json_rows']:
                result = self._client.execute(query=sql, params=params, with_column_types=True, query_id=qid)
            else:
                raise InvalidSQLOperation(f'SQL execution failed, unknown output format specified: {out}')

            # Avoid AttributeError: 'NoneType' object has no attribute 'rows' in clickhouse-driver
            try:
                self._elapsed_time = self._client.last_query.elapsed
                self._rset_rows = self._client.last_query.profile_info.rows
                self._rset_total = self._client.last_query.progress.total_rows
                self._processed_rows = self._client.last_query.progress.rows
                self._processed_bytes = self._client.last_query.progress.bytes
            except AttributeError:
                pass

            # Transform tuples to pandas dataframe
            if out == 'dataframe':
                # Start measuring elapsed time for transforming python tuples to pandas dataframe
                t_start = time.perf_counter()
                try:
                    if not as_columns:
                        as_columns = [column_name[0] for column_name in result[1]]
                    result = PandasUtils.dataframe(result[0], as_columns, index)
                except Exception:
                    print(sql)
                    raise PandasError(f'Failed to construct Pandas dataframe, check query and parameters')
                # End measuring elapsed time for pandas dataframe transformation
                t_end = time.perf_counter()
            elif out == 'json_rows':
                if not as_columns:
                    as_columns = [column_name[0] for column_name in result[1]]
                result = [dict(zip(as_columns, rec)) for rec in result[0]]
        else:
            result = sql

        # Debug info
        if self._trace > 2 and out == 'dataframe' and execute:
            print(f'Latency for pandas dataframe transformation : {round(t_end - t_start, 3)} sec.')
        if self._trace > 1:
            print(f'{self._q}\n╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌')
        if self._trace > 0:
            lqs = self.last_query_statistics
            print(f'QueryID:{lqs[1]}\nElapsed: {round(lqs[3], 3)} sec',
                  f'{lqs[2]} rows in set.\nProcessed: {lqs[4]} rows, {round(lqs[5] / 1048576, 3)} MB',
                  '\n___________________________________________________________________________')

        if result is None:
            if self._trace > 0:
                print('Done.\n╚═══════════════════════════════════════════════════════════════════════╝')

        return result

    def get_parts(self, table, hb2=None, active=True, execute=True):
        """
        :param table: clickhouse table
        :param hb2: select parts with a specific hb2 dimension (hb2 is the dim2 of the Entity/ASET key)
                    default hb2='%'
        :param active: select only active parts
        :param execute: Execute the command only if execute=True

        :return: information about parts of MergeTree tables
        """
        active_part = None
        if active is None:
            active_part = None
        elif active:
            active_part = 1
        elif not active:
            active_part = 0

        sel = f'\nSELECT table, database, partition_id, name, active, marks, rows,'
        sel += '\n        min_block_number as min_blk, max_block_number AS max_blk,'
        sel += '\n        level, toDecimal32(primary_key_bytes_in_memory/1024, 3) AS pk_mem'
        frm = 'FROM system.parts'
        if hb2 is None:
            wh = f'''WHERE database='{self._database}' AND table = '{table}' '''
        else:
            wh = f'''WHERE database='{self._database}' AND table = '{table}' AND partition_id LIKE '{hb2}-%' '''
        if active is not None:
            wh += f''' AND active = {active_part}'''
        order_by = f'ORDER BY name'

        # construct query
        query = sql_construct(select=sel, frm=frm, where=wh, order=order_by)
        cols = 'table, db, PID, name, active, marks, rows, min_blk, max_blk, level, pk_mem (KB)'.split(', ')

        return self.sql(query, as_columns=cols, index='PID', qid='Parts Command', execute=execute)

    def get_query_log(self, execute=True):
        # metadata for queries logged in the ClickHouse table with log_queries=1 setting
        sel = f'SELECT query_id AS id, user, client_hostname as host, client_name as client,'
        sel += '\n        result_rows AS in_set, toDecimal32(query_duration_ms / 1000, 3) AS sec,'
        sel += '\n        toDecimal32(memory_usage/1048576, 3) AS MEM_MB,'
        sel += '\n        read_rows as R_Rows, toDecimal32(read_bytes / 1048576, 3) AS R_MB,'
        sel += '\n        written_rows AS W_Rows, toDecimal32(written_bytes/1048576, 3) AS W_MB, query'
        frm = f'FROM system.query_log'
        wh = f'WHERE (type = 2) AND (query NOT LIKE \'%query_duration_ms%\')'
        ordtime = f'ORDER BY event_time DESC'
        # construct query
        query = sql_construct(select=sel, frm=frm, where=wh, order=ordtime)
        # execute SQL query
        cols = 'id, user, host, client, in_set, sec, MEM_MB, R_Rows, R_MB, W_Rows, W_MB, query'
        self.sql('system flush logs', qid='flush logs command', execute=execute)
        return self.sql(query, cols, qid='Query Log Command', index='id', execute=execute)

    def get_mutations(self, table, limit=None, group_by=None, execute=True):
        """
        :param table: clickhouse table
        :param group_by:
        :param limit: SQL limit
        :param execute:
        :return:
        """

        # mutations allows changing or deleting lots of rows in a table
        # return information about mutations of MergeTree tables and their progress
        sel = f'\nSELECT table, command, create_time, block_numbers.number AS blk, parts_to_do AS parts, is_done,'
        sel += '\n        latest_failed_part AS failed_at, latest_fail_time AS failed_time'
        frm = f'FROM system.mutations'
        wh = f'WHERE table = \'{table}\''
        ordtime = f'ORDER BY create_time DESC'
        if limit:
            lim = f'LIMIT {limit}'
        else:
            lim = None
        if group_by:
            grp = f'GROUP BY {group_by}'
        else:
            grp = None
        # construct query
        query = sql_construct(select=sel, frm=frm, where=wh, group_by=grp, order=ordtime, limit=lim)
        # execute SQL query
        cols = 'table, command, created_at, blk, parts, is_done, failed_at, failed_time'
        self.sql('system flush logs', qid='flush logs', execute=execute)
        return self.sql(query, cols, qid='Mutation Information Command', execute=execute)

    def get_columns(self, table=None, columns=None, fields=None, aggr=None, **kwargs):
        """
        :param table: ClickHouse table name
        :param columns: list of ClickHouse column names
        :param fields: Metadata fields for columns
        :param aggr: aggregate metadata results for the columns of a clickhouse table
        :return: metadata for clickhouse columns
        """
        order = f'ORDER BY type DESC'
        if not fields:
            fields = 'name, comment, type, ' \
                     'toDecimal32(data_compressed_bytes/1048576, 4) as Compressed_MB, ' \
                     'toDecimal32(data_uncompressed_bytes/1048576, 4) as Uncompressed_MB, ' \
                     'toDecimal32(marks_bytes/1024, 4) as marks_KB'
            order = f'ORDER BY type DESC, Compressed_MB DESC'
        # Construct query
        sel = f'SELECT {fields} '
        frm = f'FROM system.columns'
        wh = f"WHERE database='{self._database}'"
        if table:
            wh += f"AND table='{table}'"
        if columns:
            in_columns_string = "', '".join(columns)
            wh += f"AND name in ('{in_columns_string}')"
        query = sql_construct(select=sel, frm=frm, where=wh, order=order)

        # Case of collecting aggregating metadata from columns
        if aggr:
            sel = f"SELECT any('{self._database}') as db, any('{table}') as table," \
                  f"sum(Compressed_MB) as total_compressed_MB," \
                  f"sum(Uncompressed_MB) as total_uncompressed_MB, sum(marks_KB) as total_marks_KB," \
                  f"argMin(name, Compressed_MB) as min_column, min(Compressed_MB) as min_MB," \
                  f"argMax(name, Compressed_MB) as max_column, max(Compressed_MB) as max_MB," \
                  f"avg(Compressed_MB) as avg_MB"
            frm = f'FROM ( \n{query} )'
            # construct query
            query = sql_construct(select=sel, frm=frm)

        # execute SQL query
        return self.sql(query, qid=f'Get columns metadata command', **kwargs)

    def create_engine(self, table, engine, heading, partition_key=None, order_key=None, settings=None, execute=True):
        """
        :param table: name of ClickHouse engine
        :param engine: the type of clickhouse engine
        :param settings: clickhouse engine settings
        :param heading: list of field names paired with clickhouse data types
               [ ('fld1_name', 'dtype1') ,
                 ('fld2_name', 'dtype2',)
                 (  -//-     ,   -//-   )
                 ('fldN_name', 'dtypeN' )
        :param partition_key:
        :param order_key:
        :param execute:
        :return:
        """
        # Check engine passed
        if engine not in engine_types:
            raise InvalidEngine(f'Failed to create clickhouse engine {engine}')

        # join list of strings to ', \n' separated string
        structure = ', \n'.join(heading)
        query = f'CREATE TABLE {table}'
        query += f'\n({structure})'
        query += f'\nENGINE = {engine}()'
        if engine != 'Memory':
            query += f'\nPARTITION BY {partition_key}'
            query += f'\nORDER BY {order_key}'
            query += f'\nSETTINGS {settings}'

        # execute SQL query
        self.sql(f'DROP TABLE IF EXISTS {table}', qid=f'Drop Table {table}', execute=execute)
        return self.sql(query, qid=f'Create Engine {engine} Command', execute=execute)

    def optimize_engine(self, table, execute=True):
        query = f'OPTIMIZE TABLE {table} FINAL'
        # execute SQL query
        return self.sql(query, qid='Optimize Engine Command', execute=execute)

    def disconnect(self):
        self._client.disconnect()

# ***************************************************************************************
# ************************** End of ClickHouse Class ************************************
# ***************************************************************************************
