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

# ===================================================================
# Package-Modules Dependencies
# ===================================================================
import csv
import json
import os.path
import sys
import tkinter as tk

from functools import wraps
from gc import get_referents
from operator import itemgetter
from pathlib import Path
from time import gmtime, strftime
from tkinter import filedialog
from types import ModuleType, FunctionType
from IPython.display import display_html

import numpy as np
import pandas as pd
import psutil
import pyarrow as pa
import pyarrow.compute as pc
from pyarrow import csv as pa_csv, parquet as pq
from pyarrow import feather as fr

from .exceptions import PandasError

# Global variables and settings
# ToDO
pd.set_option('display.max_columns', 500)
pd.set_option('display.max_colwidth', 300)

pd.set_option('display.max_rows', 10000)
pd.set_option('display.width', 1000)

pd.set_option('precision', 3)


# Custom objects know their class.
# Function objects seem to know way too much, including modules.
# Exclude modules as well.
BLACKLIST = type, ModuleType, FunctionType


def split_comma_string(names):
    # Check parameter
    if isinstance(names, str):
        names = names.split(', ')
    elif isinstance(names, list) or isinstance(names, tuple) or (names is None):
        pass
    else:
        raise PandasError(f'Cannot split and/or use parameter {names}')
    return names


def zip_with_scalar(num, arr):
    """
    Use: to generate hyperbond (hb2, hb1), hyperatom (ha2, ha1) tuples
    :param num: scalar value
    :param arr: array of values
    :return: generator of tuples in the form (i, num) where i in arr
    """
    return ((num, i) for i in arr)


def get_size(obj):
    """sum size of object & members."""
    if isinstance(obj, BLACKLIST):
        raise TypeError('getsize() does not take argument of type: ' + str(type(obj)))
    seen_ids = set()
    size = 0
    objects = [obj]
    while objects:
        need_referents = []
        for obj in objects:
            if not isinstance(obj, BLACKLIST) and id(obj) not in seen_ids:
                seen_ids.add(id(obj))
                size += sys.getsizeof(obj)
                need_referents.append(obj)
        objects = get_referents(*need_referents)
    return bytes2mb(size)


def session_time():
    return strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())


def highlight_states(s):
    if s.P == 0:
        return ['background-color: lightgrey'] * len(s)
    elif s.S == 1:
        return ['background-color: lime'] * len(s)
    else:
        return ['background-color: white'] * len(s)


def bytes2mb(b):
    # These are now called mebibytes (MiB) according to IEC,
    # binary prefix mebi means 2^20=1048576 bytes
    return round(b/1048576, 3)


def sql_construct(select, frm, where=None, group_by=None, having=None, order=None, limit=None, offset=None):
    sql = f'{select}\n{frm}'
    if where:
        sql += f'\n{where}'
    if group_by:
        sql += f'\n{group_by}'
    if having:
        sql += f'\n{having}'
    if order:
        sql += f'\n{order}'
    if limit:
        sql += f'\n{limit}'
    if offset:
        sql += f'\n{offset}'
    return sql


class DSUtils(object):
    """
    Data Structure Utils Class
    """
    @staticmethod
    def pyarrow_record_batch_to_table(batch):
        return pa.Table.from_arrays(batch.columns, batch.schema.names)

    @staticmethod
    def pyarrow_sort(array, ascending=True):
        """
        :param array: PyArrow Array
        :param ascending:
        :return:
        """
        if ascending:
            sort_indices = pc.call_function("sort_indices", [array])
        else:
            sort_indices = pc.call_function("sort_indices", [array]).to_numpy()[::-1]

        return array.take(sort_indices)

    @staticmethod
    def pyarrow_dtype_from_string(dtype, dictionary=False, precision=9, scale=3):
        """
        :param dtype: string that specifies the PyArrow data type
        :param dictionary: pyarrow dictionary data type, i.e. pa.dictionary(pa.int32(), pa.vtype())
        :param precision: for decimal128bit width arrow data type (number of digits in the number - integer+fractional)
        :param scale: for decimal128bit width arrow data type (number of digits for the fractional part)
        :return: pyarrow data type from a string
        """
        if dictionary:
            return getattr(pa, 'dictionary')(pa.int32(), DSUtils.pyarrow_dtype_from_string(dtype))
        else:
            if dtype == 'decimal128':
                return getattr(pa, dtype)(precision, scale)
            else:
                return getattr(pa, dtype)()

    @staticmethod
    def pyarrow_get_dtype(arr):
        """
        :param arr: PyArrow 1d Array either dictionary encoded or not
        :return: value type of PyArrow array elements
        """
        if 'dictionary' in arr.type.__str__():
            pa_vtype = arr.type.value_type.__str__()
        else:
            pa_vtype = arr.type.__str__()

        return pa_vtype

    @staticmethod
    def pyarrow_table_to_record_batch(table):
        """
        :param table: PyArrow Table
        :return: PyArrow RecordBatch
        """
        dict_arrays = [DSUtils.pyarrow_chunked_to_dict(col) for col in table.columns]
        return pa.RecordBatch.from_arrays(dict_arrays, table.schema.names)

    @staticmethod
    def pyarrow_chunked_to_dict(chunked_array):
        """
        :param chunked_array: PyArrow ChunkedArray
        :return: PyArrow Array / DictionaryArray
        """
        indices = list(range(chunked_array.length()))
        return chunked_array.take(indices).chunk(0)

    @staticmethod
    def pyarrow_dict_to_arr(dict_array):
        """
        :param dict_array: PyArrow DictionaryArray
        :return: PyArrow 1d Array
        """
        arr_type = DSUtils.pyarrow_get_dtype(dict_array)
        return dict_array.cast(arr_type)

    @staticmethod
    def pyarrow_vtype_to_numpy_vtype(arr):
        """
        :param arr: PyArrow 1d Array
        :return: NumPy value type that is equivalent of PyArrow value type
        """
        pa_vtype = DSUtils.pyarrow_get_dtype(arr)
        if pa_vtype == 'string':
            np_vtype = 'str'
        elif pa_vtype == 'float':
            np_vtype = 'float32'
        elif pa_vtype == 'double':
            np_vtype = 'float64'
        else:
            np_vtype = pa_vtype

        return np_vtype

    @staticmethod
    def pyarrow_to_numpy(pa_arr):
        """
        :param pa_arr: PyArrow 1d Array or DictionaryArray
        :return: NumPy 1d array
        """
        vtype = DSUtils.pyarrow_vtype_to_numpy_vtype(pa_arr)
        if vtype in ['int8', 'uint8', 'int16', 'uint16']:
            dtype = 'float16'
        elif vtype in ['int32', 'uint32']:
            dtype = 'float32'
        elif vtype in ['int64', 'uint64']:
            dtype = 'float64'
        else:
            dtype = vtype

        # Check if it is a dictionary array and convert it to normal 1d PyArrow Array
        if type(pa_arr) == pa.DictionaryArray:
            arr = DSUtils.pyarrow_dict_to_arr(pa_arr)
        else:
            arr = pa_arr

        # If it is a string array
        if dtype == 'str':
            np_arr = arr.to_numpy(zero_copy_only=False).astype(dtype)
            np_arr[np_arr == 'None'] = np.NaN
            # Warning: when we convert the object data type to string data type
            # None or nan missing values are converted to strings 'None', 'nan'
        elif dtype == 'bool':
            np_arr = arr.to_numpy(zero_copy_only=False)
        # if there are missing values in the array
        elif pa_arr.null_count:
            np_arr = arr.to_numpy(zero_copy_only=False).astype(dtype)
        else:
            np_arr = arr.to_numpy(zero_copy_only=True).astype(dtype)

        return np_arr

    @staticmethod
    def numpy_to_pyarrow(np_arr, dtype=None, dictionary=True):
        """
            :param np_arr: numpy 1d array that represents a table column of data values of the same type
            :param dtype: data type
            :param dictionary: whether to use dictionary encoded form or not
            :return: pyarrow array representation of `arr`
            """
        if dtype:
            arr = pa.array(np_arr, type=dtype)
        else:
            arr = pa.array(np_arr)

        if dictionary:
            return arr.dictionary_encode()
        else:
            return arr

    @staticmethod
    def numpy_sorted_index(arr, adj=False, freq=False):
        """
        :param arr: numpy 1d array that represents a table column of data values of the same type
                    in the case of numpy array with string values and missing data,
                    null values must be represented with np.NaN
        :param adj: if True return adjacency lists
        :param freq: if True return frequencies

        :return:
            i) secondary index, i.e. unique values of `arr` in ascending order without NaN (null)
            ii) for each unique value calculate
                a) list of primary key indices, i.e. pointers, to all rows of the table that contain that value
                    also known as adjacency lists in Graph terminology
                b) count the number of rows that contain that value,
                    also known as database cardinality (selectivity)
                    also known as frequency in associative engine
        """
        # String case
        if 'U' in arr.dtype.str:
            # Find the indices that would sort the array
            idx = np.argsort(arr)
            # Create a mask to filter indices
            mask = arr[idx] == 'nan'
            # Find the indices that sort the array skipping those that are nan (null)
            idx_pk = idx[~mask]
        # Numeric cases
        else:
            # Count null values
            cnt = np.count_nonzero(~np.isnan(arr))
            # Find the indices that sort the array and trim those that point to null values
            idx_pk = np.argsort(arr)[:cnt]

        # Find unique elements in the secondary index and also return
        # i) The indices of the first occurrences of the unique values in the original array (used in np.split)
        # iii) The number of times each of the unique values comes up in the original array
        if not freq and not adj:
            usec_ndx = np.unique(arr[idx_pk])
            return usec_ndx
        elif freq and adj:
            usec_ndx, split_ndx, cnt_arr = np.unique(arr[idx_pk], return_index=True, return_counts=True)
            adj_list = np.split(idx_pk, split_ndx)[1:]
            return usec_ndx, cnt_arr.astype('uint32'), adj_list
        elif freq:
            usec_ndx, cnt_arr = np.unique(arr[idx_pk], return_counts=True)
            return usec_ndx, cnt_arr.astype('uint32')
        elif adj:
            usec_ndx, split_ndx = np.unique(arr[idx_pk], return_index=True)
            adj_list = np.split(idx_pk, split_ndx)[1:]
            return usec_ndx, adj_list


class FileUtils(object):
    @staticmethod
    def change_cwd(fpath):
        return os.chdir(fpath)

    @staticmethod
    def get_cwd():
        return os.getcwd()

    @staticmethod
    def get_full_path_filename(p, f):
        return os.path.join(FileUtils.get_full_path(p), f)

    @staticmethod
    def get_full_path(path):
        return os.path.join(FileUtils.get_cwd(), path)

    @staticmethod
    def get_full_path_parent(path):
        return Path(path).parent

    @staticmethod
    def get_filenames(path, extension='json', window_title='Choose files', gui=False, select=None):
        if gui:
            root = tk.Tk()
            root.withdraw()
            root.call('wm', 'attributes', '.', '-topmost', True)

            # Get filenames with extension .ext inside a folder located at relative _path
            fullpath = FileUtils.get_full_path(path)
            dialogdic = {'initialdir': fullpath, 'title': window_title}

            if extension in ['json', 'JSON']:
                dialogdic['filetypes'] = (
                    ("json files", "*.json"), ("csv files", "*.csv"), ("tsv files", "*.tsv"), ("all files", "*.*"))
            elif extension in ['csv', 'CSV']:
                dialogdic['filetypes'] = (
                    ("csv files", "*.csv"), ("tsv files", "*.tsv"), ("json files", "*.json"), ("all files", "*.*"))
            elif extension in ['tsv', 'TSV']:
                dialogdic['filetypes'] = (
                    ("tsv files", "*.tsv"), ("csv files", "*.csv"), ("json files", "*.json"), ("all files", "*.*"))
            elif extension == 'all':
                dialogdic['filetypes'] = (
                    ("all files", "*.*"), ("tsv files", "*.tsv"), ("csv files", "*.csv"), ("json files", "*.json"))

            # these are full path filenames
            fullpath_filenames = filedialog.askopenfilenames(**dialogdic)
            # return only filenames
            filenames = [os.path.basename(fnam) for fnam in fullpath_filenames]
        else:
            # Get filenames with extension .ext inside a folder located at relative _path
            full_path = FileUtils.get_full_path(path)
            if extension == 'all':
                filenames = [file for file in os.listdir(full_path)]
            else:
                ext = '.' + extension.lower()
                filenames = [file for file in os.listdir(full_path) if file.endswith(ext)]
            if select:
                filenames = itemgetter(*select)(filenames)

        return filenames

    @staticmethod
    def flatfile_drop_extention(fname):
        return os.path.splitext(fname)[0]

    @staticmethod
    def flatfile_delimiter(file_type):
        """
        :param file_type: CSV, TSV these have default delimiters ',' and '\t' respectively
        :return: default delimiter or the specified delimiter in the argument
        """
        if file_type == 'CSV':
            sep = ','
        elif file_type == 'TSV':
            sep = '\t'
        else:
            raise Exception(f'Failed to read file header: Unknown file type {file_type}')
        return sep

    @staticmethod
    def flatfile_header(file_type, file_location, delimiter=None):
        """
        :param file_type: CSV, TSV these have default delimiters ',' and '\t' respectively
        :param delimiter: 1-character string specifying the boundary between fields of the record
        :param file_location: full path location of the file with an extension (.tsv, .csv)
        :return: field names in a list
        """
        with open(file_location, 'r') as infile:
            reader = csv.DictReader(infile, delimiter=FileUtils.flatfile_delimiter(file_type))
            return reader.fieldnames

    @staticmethod
    def flatfile_to_python_lists(file_type, file_location, nrows=10, skip_rows=1, delimiter=None):
        """
        :param file_type: CSV, TSV these have default delimiters ',' and '\t' respectively
        :param delimiter: 1-character string specifying the boundary between fields of the record
        :param file_location: full path location of the file with an extension (.tsv, .csv)
        :param nrows: number of rows to read from the file
        :param skip_rows: number of rows to skip, default 1 skip the header of the file
        :return: rows of the file as python lists
        """
        with open(file_location, 'r') as infile:
            reader = csv.reader(infile, delimiter=FileUtils.flatfile_delimiter(file_type))
            if skip_rows:
                [next(reader) for _ in range(skip_rows)]
            return [next(reader) for _ in range(nrows)]

    @staticmethod
    def flatfile_to_pyarrow_table(file_type, file_location, select=None, as_columns=None, as_types=None,
                                  partition_size=None, limit=None, offset=None, skip=0,
                                  delimiter=None, nulls=None):
        """
        Read columnar data from CSV files
        https://arrow.apache.org/docs/python/csv.html

        :param file_type: CSV, TSV these have default delimiters ',' and '\t' respectively
        :param file_location: full path location of the file
        :param delimiter: 1-character string specifying the boundary between fields of the record
        :param nulls: list of strings that denote nulls e.g. ['\\N']
        :param partition_size: number of records to use for each partition or target size of each partition, in bytes
        :param select: list of column names to include in the pyarrow Table, default None (all columns)
        :param as_columns: user specified column names for pandas dataframe (list of strings)
        :param as_types: Map column names to column types (disabling type inference on those columns)
        :param limit: limit on the number of rows to return
        :param offset: exclude the first number of rows
                        Notice: do not confuse `offset` with `skip`, offset is used after we read the table
        :param skip: number of rows to skip at the start of the flat file

        :return: pyarrow in-memory table
        """
        # CSV, TSV default delimiters
        sep = FileUtils.flatfile_delimiter(file_type)
        # Overwrite default delimiter....
        if delimiter:
            sep = delimiter

        if select:
            columns = split_comma_string(select)
        else:
            columns = None

        if as_columns:
            column_names = split_comma_string(as_columns)
        else:
            column_names = None

        if as_types:
            column_types = split_comma_string(as_types)
            column_types = dict(zip(column_names, column_types))
            # Notice usually there is a header in the file,
            # you must set skip=1 to avoid conversion on the header row
        else:
            column_types = None

        tbl = pa_csv.read_csv(file_location,
                              parse_options=pa_csv.ParseOptions(delimiter=sep),
                              convert_options=pa_csv.ConvertOptions(include_columns=columns, column_types=column_types,
                                                                    null_values=nulls, strings_can_be_null=True),
                              read_options=pa_csv.ReadOptions(skip_rows=skip, autogenerate_column_names=False,
                                                              use_threads=True, column_names=column_names,
                                                              block_size=partition_size)
                              )

        if limit and not offset:
            return tbl[0:limit]
        elif limit and offset:
            return tbl[offset:limit+offset]
        else:
            return tbl

    @staticmethod
    def flatfile_to_pandas_dataframe(file_type, file_location, select=None, as_columns=None, as_types=None, index=None,
                                     partition_size=None, limit=None, offset=None,
                                     delimiter=None, nulls=None, **pandas_kwargs):
        """
        Read rows from flat file and convert them to pandas dataframe with pandas.read_csv
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html

        :param file_type: CSV, TSV these have default delimiters ',' and '\t' respectively
        :param file_location: full path location of the file
        :param delimiter: 1-character string specifying the boundary between fields of the record
        :param nulls: list of strings that denote nulls e.g. ['\\N']
        :param partition_size: number of records to use for each partition or target size of each partition, in bytes
        :param select: use a subset of columns from the flat file
        :param as_columns: user specified column names for pandas dataframe (list of strings)
        :param as_types: dictionary with column names as keys and data types as values
                this is used when we read data from flat files and we want to disable type inference on those columns
        :param index: column names to be used in pandas dataframe index
        :param limit: limit on the number of records to return
        :param offset: exclude the first number of rows
                       Notice: do not confuse offset with the number of rows to skip at the start of the flat file
                       but in pandas.read_csv offset can also be used as skiprows

        :param pandas_kwargs: other arguments of pandas `read_csv` method
        :return: pandas dataframe

        Example of read_cvs():
        read_csv(source, sep='|', index_col=False, nrows=10, skiprows=3, header = 0
                 usecols=['catsid', 'catpid', 'catcost', 'catfoo', 'catchk'],
                 dtype={'catsid':int, 'catpid':int, 'catcost':float, 'catfoo':float, 'catchk':bool},
                 parse_dates=['catdate'])
        """
        # CSV, TSV default delimiters
        pandas_kwargs['sep'] = FileUtils.flatfile_delimiter(file_type)
        # Overwrite default delimiter....
        if delimiter:
            pandas_kwargs['sep'] = delimiter

        # map the file object directly onto memory and access the data directly from there.
        # Using this option can improve performance because there is no longer any I/O overhead
        pandas_kwargs['memory_map'] = True

        # Alternatively for very large files iterate lazily
        # rather than reading the entire file into memory specify `partition_size` parameter to read a number of rows
        # the return value will be an iterable object of type TextFileReader:
        #
        # chunk_reader.get_chunk()   # Read 100 rows
        # chunk_reader.get_chunk()   # Read the next 100 rows
        # chunk_reader.get_chunk(4)  # Read the next 4 rows
        if select:
            pandas_kwargs['usecols'] = split_comma_string(select)

        if as_columns:
            pandas_kwargs['names'] = split_comma_string(as_columns)
            pandas_kwargs['header'] = None

        if as_types:
            as_types = split_comma_string(as_types)
            pandas_kwargs['dtype'] = dict(zip(as_columns, as_types))

        if index:
            pandas_kwargs['index_col'] = split_comma_string(index)

        if limit:
            pandas_kwargs['nrows'] = limit

        if offset:
            pandas_kwargs['skiprows'] = offset

        if partition_size:
            pandas_kwargs['chunksize'] = partition_size

        if nulls:
            pandas_kwargs['na_values'] = nulls

        return pd.read_csv(file_location, **pandas_kwargs)

    @staticmethod
    def parquet_metadata(source, **pyarrow_kwargs):
        return pq.read_metadata(where=source, **pyarrow_kwargs)

    @staticmethod
    def parquet_to_arrow_schema(source, **pyarrow_kwargs):
        return pq.read_schema(where=source, **pyarrow_kwargs)

    @staticmethod
    def parquet_to_arrow_table(file_location, select=None, limit=None, offset=None,
                               arrow_encoding=False, **pyarrow_kwargs):
        """
        This is using pyarrow.parquet.read_table()
        https://arrow.apache.org/docs/python/generated/pyarrow.parquet.read_table.html

        :param file_location: full path location of the file
        :param select: use a subset of columns from parquet file
        :param limit: limit on the number of records to return
        :param offset: exclude the first number of rows
                       Notice: do not confuse offset with the number of rows to skip at the start of the flat file
                       but in pandas.read_csv offset can also be used as skiprows
        :param arrow_encoding: PyArrow dictionary encoding
        :param pyarrow_kwargs: other parameters that are passed to pyarrow.parquet.read_table
        :return:
        """
        if select:
            columns = split_comma_string(select)
        else:
            columns = None

        if arrow_encoding:
            # It has limitations for numeric types. It is only supported for BYTE_ARRAY storage
            pyarrow_kwargs['read_dictionary'] = columns

        tbl = pq.read_table(source=file_location, columns=columns, **pyarrow_kwargs)

        if limit and not offset:
            return tbl[0:limit]
        elif limit and offset:
            return tbl[offset:limit+offset]
        else:
            return tbl

    @staticmethod
    def feather_to_arrow_schema(source):
        tbl = FileUtils.feather_to_arrow_table(file_location=source, limit=3)
        return tbl.schema

    @staticmethod
    def feather_to_arrow_table(file_location, select=None, limit=None, offset=None, **pyarrow_kwargs):
        """
        This is using pyarrow.feather.read_table()
        https://arrow.apache.org/docs/python/generated/pyarrow.feather.read_table.html#pyarrow.feather.read_table

        :param file_location: full path location of the file
        :param select: use a subset of columns from feather file
        :param limit: limit on the number of records to return
        :param offset: exclude the first number of rows
                       Notice: do not confuse offset with the number of rows to skip at the start of the flat file
                       but in pandas.read_csv offset can also be used as skiprows
        :param pyarrow_kwargs: other parameters that are passed to pyarrow.feather.read_table
        :return:
        """
        if select:
            columns = split_comma_string(select)
        else:
            columns = None

        tbl = fr.read_table(source=file_location, columns=columns, **pyarrow_kwargs)

        if limit and not offset:
            return tbl[0:limit]
        elif limit and offset:
            return tbl[offset:limit + offset]
        else:
            return tbl

    @staticmethod
    def pyarrow_table_to_feather(table, file_location, **feather_kwargs):
        """
        Write a Table to Feather format
        :param table: pyarrow Table
        :param file_location: full path location of the feather file
        :param feather_kwargs:
        https://arrow.apache.org/docs/python/generated/pyarrow.feather.write_feather.html#pyarrow.feather.write_feather
        :return:
        """
        fr.write_feather(df=table, dest=file_location, **feather_kwargs)
        return file_location

    @staticmethod
    def pyarrow_table_to_parquet(table, file_location, **pyarrow_kwargs):
        """
        Write a Table to Parquet format
        :param table: pyarrow Table
        :param file_location: full path location of the parquet file
        :param pyarrow_kwargs: row_group_size, version, use_dictionary, compression (see...
        https://pyarrow.readthedocs.io/en/latest/generated/pyarrow.parquet.write_table.html#pyarrow.parquet.write_table
        :return:
        """
        pq.write_table(table=table, where=file_location, **pyarrow_kwargs)
        return file_location

    @staticmethod
    def pyarrow_write_record_batch(record_batch, file_location):
        """
        :param record_batch: PyArrow RecordBatch
        :param file_location:
        :return:
        """
        ipc = pa.RecordBatchFileWriter(file_location, record_batch.schema)
        ipc.write_batch(record_batch)
        ipc.close()

    @staticmethod
    def pyarrow_read_record_batch(file_location, table=False):
        """
        :param file_location:
        :param table:
        :return: Either PyArrow RecordBatch, or PyArrow Table if table=True
        """
        ipc = pa.RecordBatchFileReader(file_location)
        if table:
            return ipc.read_all()
        else:
            return ipc.get_record_batch(0)

    @staticmethod
    def write_json(data, fname):
        with open(fname, 'w') as outfile:
            json.dump(data, outfile, indent=3)
        return True

    @staticmethod
    def json_to_dict(fname):
        with open(fname) as f:
            return json.load(f)


class PandasUtils(object):
    """
    pandas dataframe utility methods
    """
    @staticmethod
    def dataframe(iterable, columns=None, ndx=None):
        """
        :param iterable: e.g. list like objects
        :param columns: comma separated string or list of strings
                        labels to use for the columns of the resulting dataframe
        :param ndx: comma separated string or list of strings
                    column names to use for the index of resulting dataframe
        :return: pandas dataframe with an optional index
        """
        if columns:
            columns = split_comma_string(columns)

        # construct dataframe without the index
        df = pd.DataFrame(data=iterable, columns=columns)

        if ndx:
            # set
            df.set_index(split_comma_string(ndx), inplace=True)

        return df

    @staticmethod
    def dataframes_to_html(*df_stylers):
        html_repr = ''
        for styler in df_stylers:
            html_repr += styler.render() + "\xa0\xa0\xa0"

        return display_html(html_repr, raw=True)

    @staticmethod
    def dataframe_to_pyarrow_table(df, columns=None, schema=None, index=False):
        """
        :param df: pandas dataframe
        :param columns:  List of column to be converted. If None, use all columns
        :param schema: the expected pyarrow schema of the pyarrow Table
        :param index: Whether to store the index as an additional column in the resulting Table.

        :return: pyarrow.Table
        """
        return pa.Table.from_pandas(df=df, columns=columns, schema=schema, preserve_index=index)

    @staticmethod
    def dataframe_cardinality(df):
        # cardinality refers to the uniqueness of data values
        # contained in a particular column of a dataframe
        return dict(zip(df.nunique().keys(), df.nunique().values))

    @staticmethod
    def dataframe_selectivity(df):
        # from decimal import Decimal
        # round(Decimal(1091544/2788597), 6)
        # selectivity is a measure of how much variety there is in the values of a given dataframe column
        # in relation to the total number of rows in a given dataframe
        keys = df.nunique().keys()
        vals = [int(round(val / df.shape[0] * 100)) for val in df.nunique().values]
        return dict(zip(keys, vals))

    @staticmethod
    def dataframe_memory_usage(df, deep=False):
        result = df.memory_usage(deep=deep).apply(bytes2mb)
        total_mb = result.sum()
        return result, print(f'Total MiB: {total_mb}')

    @staticmethod
    def dataframe_concat_columns(df1, df2):
        return pd.concat([df1, df2], axis=1)

    @staticmethod
    def dict_to_dataframe(d, labels):
        return pd.DataFrame.from_dict(d, orient='index', columns=labels)


class DotDict(dict):
    """
    dot.notation access to dictionary attributes

    Example:
    person_dict = {'first_name': 'John', 'last_name': 'Smith', 'age': 32}
    address_dict = {'country': 'UK', 'city': 'Sheffield'}

    person = DotDict(person_dict)
    person.address = DotDict(address_dict)

    print(person.first_name, person.last_name, person.age, person.address.country, person.address.city)
    """
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


class MemStats(object):
    """
    Compare memory statistics with free -m
    Units are in MiB memibytes, 1 MiB = 2^20 bytes
    """

    def __init__(self):
        self._mem_avail1 = self.available
        self._mem_avail2 = None

    @property
    def difference(self):
        self._mem_avail2 = self.available
        result = self._mem_avail1 - self._mem_avail2
        self._mem_avail1 = self._mem_avail2
        return print(f'Memory Difference: {round(result, 1)} MiB')

    @property
    def mem(self):
        return psutil.virtual_memory()

    @property
    def cpu(self):
        return psutil.cpu_percent()

    @property
    def total(self):
        return bytes2mb(self.mem.total)

    @property
    def used(self):
        return bytes2mb(self.mem.used)

    @property
    def available(self):
        # Compare with available memory from UNIX command free -m
        return bytes2mb(self.mem.available)

    @property
    def buffers(self):
        return bytes2mb(self.mem.buffers)

    @property
    def cached(self):
        return bytes2mb(self.mem.cached)

    @property
    def free(self):
        return bytes2mb(self.mem.free)

    def __repr__(self):
        avpercent = self.available * 100 / self.total
        # usedpercent = self.used * 100 / self.total
        available_str = f'Memory(available:{self.available} MiB ({round(avpercent,1)}%),'
        total_used_str = f'total used:{round(self.total-self.available, 2)} MiB ({self.mem.percent}%))'
        return available_str+total_used_str

    def print_stats(self):
        print(f'System Memory ({self.mem.percent} %), CPU ({self.cpu}%))')
        print('====================================')
        print(f'Total      = {self.total} MiB')
        print(f'Used       = {self.used} MiB')
        print(f'Available  = {self.available} MiB')
        print(f'Buffers    = {self.buffers} MiB')
        print(f'Cached     = {self.cached} MiB')
        print(f'Free       = {self.free} MiB')


# ===========================================================================================
# Generative Classes
# ===========================================================================================
class GenerativeBase(object):
    """
    http://derrickgilland.com/posts/introduction-to-generative-classes-in-python/
    A Python Generative Class is defined as
    a class that returns or clones, i.e. generates, itself when accessed by certain means
    This type of class can be used to implement method chaining
    or to mutate an object's state without modifying the original class instance.
    """
    def _generate(self):
        s = self.__class__.__new__(self.__class__)
        s.__dict__ = self.__dict__.copy()
        return s

# ***************************************************************************************
# ************************** End of GenerativeBase Class ***********************
# ***************************************************************************************


def _generative(func):
    """
    :param func: is the method of the class to use in chaining
    :return: decorator to use in chaining methods
    """
    @wraps(func)
    def decorator(self, *args, **kw):
        self = self._generate()
        func(self, *args, **kw)
        return self
    return decorator


# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
# ***************************************************************************************
#                 =======   End of hypermorph_generative Module =======
# ***************************************************************************************
# \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
