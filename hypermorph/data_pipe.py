import pyarrow as pa
from types import GeneratorType

from . import db_types, flat_file_types
from .utils import PandasUtils, FileUtils, DSUtils, split_comma_string, GenerativeBase, _generative
from .exceptions import InvalidPipeOperation


# ===========================================================================================
# Pipe for data operations
# ===========================================================================================
class DataPipe(GenerativeBase):
    """
    Implements method chaining:
    A query operation, e.g. projection, counting, filtering
    can invoke multiple method calls. Each method corresponds to a query operator such as:
    get_components.over().to_dataframe().out()

    `out()` method is always at the end of the chained generative methods to return the final result

    Each one of these operators returns an intermediate result `self.fetch`
    allowing the calls to be chained together in a single statement.

    DataPipe methods such as get_rows() are wrapped inside methods of other classes e.g. get_rows() Table(SchemaNode)
    so that when they are called from these methods the result can be chained to other methods of DataPipe
    In that way we implement easily and intuitively transformations and conversion to multiple output formats

    Notice: we distinguish between two different execution types according to the evaluation of the result
        i) Lazy evaluation, see for example to_***() methods
        ii) Eager evaluation

    This module has a dual combined purpose:
        i)  perform transformations from one data structure to another data structure
        ii) load data into volatile memory (RAM, DRAM, SDRAM, SRAM, GDDR) or
            import data into non-volatile storage (NVRAM, SSD, HDD, Database)
            with a specific format e.g. parquet, JSON, ClickHouse MergeTree engine table, MYSQL table, etc...

    Transformation, importing and loading operations are based on
    pyarrow/numpy library and ClickHouse columnar DBMS
    """

    def __init__(self, schema_node, result=None):
        """
        :param schema_node: HyperMorph SchemaNode object
        :param result: any result that is returned from generative operations (methods)
        """
        self.fetch = result

        # Composition object and its instance variables
        self._node = schema_node
        self._schema = schema_node.schema

        # projection column names
        self._project = None
        self._as_names = None
        self._as_types = None

        # Query string
        self._sql_query = None

        # get_rows parameters
        self._nparts, self._partsize, self._limit, self._offset, self._index = [None]*5

        # The type of operation that is permitted to be executed
        self._operation = 'UNDEFINED'

        # DataPipe object dunder representation method (__repr__)
        # that is modified according to the chaining of operations
        self._repr = ''

    def __repr__(self):
        return f'{self._node}.pipe{self._repr}'

    @property
    def sql_query(self):
        return self._sql_query

    @property
    def schema_node(self):
        return self._node

    @_generative
    def get_columns(self):
        """
        Wrapped in Table(SchemaNode) class
        :return: pass self.fetch to the next chainable operation
        """
        self._repr += f'.get_columns()'

        # start constructing the sql query here,
        # i.e. self._sql_query is also used as a flag to indicate that we will fetch data using SQL query
        if self._node.ctype in db_types:
            self._sql_query = f'\nSELECT '
            self.fetch = self._sql_query
        elif self._node.ctype == 'FEATHER':
            pass

    @_generative
    def get_rows(self, npartitions=None, partition_size=None):
        """
        Wrapped in Table(SchemaNode) class
        Fetch either records of an SQL table or rows of a flat file
        Notice: Specify either `npartitions` or `block_size` parameter or none of them

        :param npartitions: split the values of the index column linearly
                            slice() will have the effect of modifying accordingly the split
        :param partition_size: number of records to use for each partition or target size of each partition, in bytes

        Notice: npartitions or partition_size will perform a lazy evaluation and it will return a generator object

        :return: pass self.fetch to the next chainable operation
        """
        self._repr += f'.get_rows({npartitions}, {partition_size})'

        self._nparts = npartitions
        self._partsize = partition_size

        # get rows from a database table using SQL query
        # start constructing the sql query here,
        # i.e. self._sql_query is also used as a flag to indicate that we will fetch data using SQL query
        if self._node.ctype in db_types:
            self._sql_query = f'\nSELECT '
            self.fetch = self._sql_query

    @_generative
    def exclude(self, select=None):
        """
        :param select: Exclude columns in projection
        :return:
        """
        self._repr += f'.exclude({select})'

    @_generative
    def over(self, select=None, as_names=None, as_types=None):
        """
        Notice: over() must be present in method chaining
                when you fetch data by constructing and executing an SQL query
                in that case default projection self._project = ' * '

        :param select: projection over the selected metadata columns
        :param as_names: list of column names to use for resulting frame
                List of user-specified column names, these are used:
                i) to rename columns (SQL `as` operator)
                ii) to extend the result set with calculated columns from an expression
        :param as_types: list of data types or comma separated string of data types
                e.g. when we read data from flat files using pandas.read_csv
                and we want to disable type inference on those columns these are pandas data types
        :return: pass self.fetch to the next chainable operation
        """
        self._repr += f'.over({select})'
        self._as_names = as_names
        self._as_types = as_types
        if select:
            self._project = select

        # e.g. get_rows() method starts construction of sql_query
        if self._sql_query:
            if not select:
                self._project = ' * '
            if isinstance(self._project, list):
                self._project = ', '.join(self._project)
            self._sql_query += self._project
            self._sql_query += f'\nFROM {self._node.cname}'
            self.fetch = self._sql_query

    @_generative
    def where(self, condition=None):
        self._repr += f'.over({condition})'
        if self._sql_query and condition:
            self._sql_query += f'\nWHERE {condition}'
            self.fetch = self._sql_query

    @_generative
    def slice(self, limit=None, offset=0):
        """
        :param limit:  number of rows to return from the result set
        :param offset: number of rows to skip from the result set
        :return: SQL statement
        """
        self._repr += f'.slice({limit}, {offset})'
        self._limit = limit
        self._offset = offset
        if self._sql_query and limit:
            self._sql_query += f'\nLIMIT {limit} OFFSET {offset}'
            self.fetch = self._sql_query

    @_generative
    def order_by(self, columns):
        """
        :param columns: comma separated string
                        column names to sort by
        :return:
        """
        self._sql_query += f'\nORDER BY {columns}'
        self.fetch = self._sql_query
        self._repr += f'.sort({columns})'

    @_generative
    def to_tuples(self, trace=None):
        """
        ToDo NumPy structured arrays representation....
        :param trace: trace execution of query, i.e. print query, ellapsed time, rows in set,  etc....
        :return:
        """
        if self._sql_query:
            # Execute SQL query and return tuples
            self.fetch = self._node.sql(self._sql_query, out='tuples', trace=trace)
        else:
            raise InvalidPipeOperation(f'Failed to fetch dataframe with command {self._repr}')

    def _table_to_record_batch(self, arrow_encoding):
        """
        Use: in Parquet, flat files to transform PyArrow table
        Input: PyArrow Table
        :param arrow_encoding: PyArrow dictionary encoding
        :return: PyArrow RecordBatch
        """
        if arrow_encoding:
            column_names = self.fetch.schema.names
            columns_dict_encoded = []
            for col in self.fetch.columns:
                if 'dictionary' in col.chunk(0).type.__str__():
                    columns_dict_encoded.append(col.chunk(0))
                else:
                    columns_dict_encoded.append(col.chunk(0).dictionary_encode())
            rec_batch = pa.RecordBatch.from_arrays(columns_dict_encoded, names=column_names)
        else:
            rec_batch = self.fetch.to_batches()
        return rec_batch

    @_generative
    def to_parquet(self, path, **parquet_kwargs):
        """
        :param path: full path string of the parquet file
        :param parquet_kwargs: row_group_size, version, use_dictionary, compression (see...
        https://pyarrow.readthedocs.io/en/latest/generated/pyarrow.parquet.write_table.html#pyarrow.parquet.write_table
        :return: file_location
        """
        self.fetch = FileUtils.pyarrow_table_to_parquet(table=self.fetch, file_location=path, **parquet_kwargs)

    @_generative
    def to_feather(self, path, **feather_kwargs):
        """
        :param path: full path of the feather file
        :param feather_kwargs:
        :return: file_location
        """
        self.fetch = FileUtils.pyarrow_table_to_feather(table=self.fetch, file_location=path, **feather_kwargs)
        self._repr += f'.to_feather({path}, {feather_kwargs})'

    @_generative
    def to_batch(self, delimiter=None, nulls=None, skip=0, trace=None, arrow_encoding=True):
        """
        :param delimiter: 1-character string specifying the boundary between fields of the record
        :param nulls: list of strings that denote nulls e.g. ['\\N']
        :param skip: number of rows to skip at the start of the flat file
        :param trace: trace execution of query, i.e. print query, ellapsed time, rows in set,  etc....
        :param arrow_encoding: apply PyArrow columnar dictionary encoding

        :return: PyArrow RecordBatch with optionally dictionary encoded columns
        """
        if type(self.fetch) == pa.Table:
            self.fetch = DSUtils.pyarrow_table_to_record_batch(self.fetch)
        elif self._node.ctype in flat_file_types:
            # CSV, TSV flat files
            self.fetch = FileUtils.flatfile_to_pyarrow_table(file_type=self._node.ctype, file_location=self._node.path,
                                                             select=self._project, as_columns=self._as_names,
                                                             as_types=self._as_types, partition_size=self._partsize,
                                                             limit=self._limit, offset=self._offset, skip=skip,
                                                             delimiter=delimiter, nulls=nulls)
            # PyArrow RecordBatch with PyArrow dictionary encoding
            if arrow_encoding:
                self.fetch = self._table_to_record_batch(arrow_encoding=True)
        elif self._sql_query:
            # Execute SQL query and return PyArrow Table or PyArrow RecordBatch with optional arrow dictionary encoding
            self.fetch = self._node.sql(self._sql_query, trace=trace, as_columns=self._as_names,
                                        out='batch', arrow_encoding=arrow_encoding)
        else:
            raise InvalidPipeOperation(f'Failed to convert data to PyArrow RecordBatch')
        self._repr += f'.to_batch()'

    @_generative
    def to_table(self, delimiter=None, nulls=None, skip=0, trace=None, arrow_encoding=True):
        """
        Notice1: This is a transformation from a row layout to a column layout, i.e. chained to get_rows() method
                 Dictionary encoded columnar layout is a fundamental component of HyperMorph associative engine.
        Notice2: The output is a PyArrow Table data structure with a columnar layout, NOT a row layout,
        Notice3: method is also used when we fetch columns directly from a columnar data storage e.g.
                 ClickHouse columnar database, parquet files, i.e. chained to get_columns() method

        :param delimiter: 1-character string specifying the boundary between fields of the record
        :param nulls: list of strings that denote nulls e.g. ['\\N']
        :param skip: number of rows to skip at the start of the flat file
        :param trace: trace execution of query, i.e. print query, ellapsed time, rows in set,  etc....
        :param arrow_encoding: apply PyArrow columnar dictionary encoding

        :return: PyArrow in-memory table with a columnar data structure with optionally dictionary encoded columns
        """
        if self._node.ctype in flat_file_types:
            # CSV, TSV flat files
            self.fetch = FileUtils.flatfile_to_pyarrow_table(file_type=self._node.ctype, file_location=self._node.path,
                                                             select=self._project, as_columns=self._as_names,
                                                             as_types=self._as_types, partition_size=self._partsize,
                                                             limit=self._limit, offset=self._offset, skip=skip,
                                                             delimiter=delimiter, nulls=nulls)
            # PyArrow Table with PyArrow dictionary encoding
            if arrow_encoding:
                # Output is PyArrow dictionary encoded Table
                rec_batch = self._table_to_record_batch(arrow_encoding=True)
                self.fetch = pa.Table.from_batches([rec_batch])
        elif self._node.ctype == 'FEATHER':
            self.fetch = FileUtils.feather_to_arrow_table(file_location=self._node.path, select=self._project,
                                                          limit=self._limit, offset=self._offset)
            # Rename columns
            if self._as_names:
                self.fetch = self.fetch.rename_columns(split_comma_string(self._as_names))
        elif self._sql_query:
            # Execute SQL query and return PyArrow Table or PyArrow RecordBatch with optional arrow dictionary encoding
            self.fetch = self._node.sql(self._sql_query, trace=trace, as_columns=self._as_names,
                                        out='table', arrow_encoding=arrow_encoding)
        else:
            raise InvalidPipeOperation(f'Failed to convert data to PyArrow Table')

        self._repr += f'.to_table(delimiter={delimiter}, nulls={nulls})'

    @_generative
    def to_dataframe(self, data=None, index=None, delimiter=None, nulls=None, trace=None):
        """
        :param data: ndarray (structured or homogeneous), Iterable, dict
        :param index: column names of the result set to use in pandas dataframe index
        :param delimiter: 1-character string specifying the boundary between fields of the record
        :param nulls: list of strings that denote nulls e.g. ['\\N']
        :param trace: trace execution of query, i.e. print query, ellapsed time, rows in set,  etc....
        :return: pandas dataframe
        """
        if type(self.fetch) == pa.Table:
            # Convert pyarrow table to pandas dataframe
            df = self.fetch.to_pandas()
            if index:
                df.set_index(split_comma_string(index), inplace=True)
        elif data:
            # Convert data to pandas dataframe
            df = PandasUtils.dataframe(data, columns=self._as_names, ndx=index)
        elif self._node.ctype in flat_file_types:
            # Convert flatfile to pandas dataframe with pandas.read_csv() method
            df = FileUtils.flatfile_to_pandas_dataframe(file_type=self._node.ctype, file_location=self._node.path,
                                                        select=self._project, as_columns=self._as_names,
                                                        as_types=self._as_types, index=index,
                                                        partition_size=self._partsize,
                                                        limit=self._limit, offset=self._offset,
                                                        delimiter=delimiter, nulls=nulls)
        elif self._sql_query:
            # Execute SQL query and return pandas dataframe
            df = self._node.sql(self._sql_query, partition_size=self._partsize, as_columns=self._as_names,
                                index=index, trace=trace)
        else:
            raise InvalidPipeOperation(f'Failed to fetch dataframe with command {self._repr}')

        self.fetch = df
        self._repr += f'.to_dataframe({data}, {index})'

    def out(self, lazy=False):
        """
        We distinguish between two cases, eager vs lazy evaluation.
        This is particularly useful when we deal with very large dataframes that do not fit in memory

        :param :lazy:

        :return: use `out()` method at the end of the chained generative methods to return the
        output of SchemaNode objects displayed with the appropriate specified format and structure
        """
        #
        # return a generator result, i.e. lazy=True
        #
        if isinstance(self.fetch, GeneratorType):
            # Eager evaluation
            if self._sql_query and not lazy:
                if self._index:
                    return [df.set_index(self._index) for df in self.fetch]
                else:
                    return [df for df in self.fetch]
            else:
                # Lazy evaluation
                return self.fetch
        else:
            return self.fetch

# ***************************************************************************************
#   ************************** End of DataPipe Class ********************************
# ***************************************************************************************
