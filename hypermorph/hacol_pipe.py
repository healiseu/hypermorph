
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import numpy as np
from types import GeneratorType
from pandas import Series
from .utils import GenerativeBase, _generative, DSUtils, zip_with_scalar
from .exceptions import InvalidPipeOperation

# List of defined operations
oplist = ['Transformation', 'Filtering', 'Restriction', 'Slicing',
          'Counting', 'MemoryUsage', 'Frequency', 'Sum', 'Average',
          'Projection', 'Intersection', 'Dictionary']
scalar_oplist = ['equal', 'not_equal', 'less', 'less_equal', 'greater', 'greater_equal']


# ===========================================================================================
# Pipeline operations for HAtomCollection
# ===========================================================================================
class HACOLPipe(GenerativeBase):

    def __init__(self, hacol, result=None):
        """
        :param HACOL hacol: HyperAtom Collection
        :param result:
        """
        self.fetch = result

        # Composition object
        self._hacol = hacol
        self._attribute = self._hacol.attribute
        self._filtered = self._hacol.filtered
        self._dim3 = self._hacol.dim3
        self._dim2 = self._hacol.dim2
        self._name = self._hacol.name
        self._vtype = self._hacol.vtype

        # HACOLPipe object dunder representation method (__repr__)
        # that is modified according to the chaining of operations
        self._repr = ''

        # This instance variable controls the type of operation that is permitted to be executed
        self._operation = None

        self._pandas_dfcolumns = None
        self._pandas_dfindex = None

    def __repr__(self):
        return f'{self._hacol.type}({self._dim3}, {self._dim2})[{self._name}].pipe{self._repr}'

    @_generative
    def start(self):
        """
        This is used as the first method in a chain of other methods where we
        set the filtered/unfiltered data
        pipeline methods slice(), to_array(), to_numpy(), to_series() start here
        :return: DictionaryArray either in filtered or unfiltered state
        """
        if self._hacol.is_filtered():
            self.fetch = self._hacol.filtered_data
        else:
            self.fetch = self._hacol.data

    @_generative
    def count(self, dataframe=True):
        """
        :param dataframe: flag to display output with a Pandas dataframe
        :return: number of values in filtered/unfiltered state
                 number of hatoms in filtered/unfiltered state
        """
        if self._hacol.filtered:
            indices = self.fetch.indices
            # Filter valid indices
            valid_indices = indices.filter(indices.is_valid())
            self.fetch = [len(valid_indices.unique()), len(indices)]
        else:
            self.fetch = [self._hacol.hatoms_unique, self._hacol.values_included]

        if dataframe:
            columns = ['Count']
            index = ['Filtered Atoms', 'Filtered Values', '---------------',
                     'Atoms', 'Values Valid', 'Values Missing', 'Values Total']
            data = (self.fetch[0], self.fetch[1], '---------',
                    self._hacol.hatoms_unique, self._hacol.values_valid,
                    self._hacol.values_missing, self._hacol.values_total)
            self.fetch = pd.DataFrame(data, columns=columns, index=index)

        self._operation = 'Counting'
        self._repr += f'.count()'

    def _execute_scalar_operation(self, op, number):
        """
        :param op: scalar operation to apply
        :param number: a scalar value specified by the user
        :return:
        """
        if op not in scalar_oplist:
            raise InvalidPipeOperation(f'Scalar operation {op} is not defined')
        array = self.fetch.cast(self._vtype)
        scalar = pa.scalar(number, type=self._vtype)
        scalar_fun = getattr(pc, op)
        mask = scalar_fun(array, scalar)
        return mask

    @staticmethod
    def _parse_condition(cond):
        """
        :param str cond: string that represents a condition in where() operator
        :return: string name of the operation and the numerical value (scalar)
        """
        pos = None
        if '>=' in cond:
            op = 'greater_equal'
            pos = cond.find('>=')+2
        elif '<=' in cond:
            op = 'less_equal'
            pos = cond.find('<=')+2
        elif '<' in cond:
            op = 'less'
            pos = cond.find('<')+1
        elif '>' in cond:
            op = 'greater'
            pos = cond.find('>')+1
        elif '=' in cond:
            op = 'equal'
            pos = cond.find('=')+1
        elif '!=' in cond:
            op = 'not_equal'
            pos = cond.find('!=')+2
        else:
            op = None

        if op:
            return op, float(cond[pos:])
        else:
            return None, None

    @_generative
    def where(self, condition='$v'):
        """
        Example: phys.q.where('city like ATLANTA')
        Notice: Entering where() method, self.fetch = self._hacol.filtered_data
                Thus pc.match_substring(), pc.greater(), pc.equal() etc... are applied to either
                already filtered or unfiltered (self._hacol.filtered_data = self._hacol.data) DictionaryArray

        :param condition:
        :return: PyArrow Boolean Array mask (self.fetch) that is used in filter()
                 it also returns boolean mask to calls from ASETPipe.where(), ASETPipe.And() methods
        """
        # Scalar operations
        scalar_op, val = self._parse_condition(condition)
        if scalar_op:
            self.fetch = self._execute_scalar_operation(scalar_op, val)
        # like operator can also be passed using the method like()
        elif 'like' in condition:
            pattern = condition[condition.find('like')+4:].strip()
            self.fetch = pc.match_substring(self.fetch.cast(self._vtype), pattern)
        else:
            pass    # self.fetch = self._hacol.filtered_data

        if not self._operation:
            self._operation = 'Restriction'

        self._repr += f'.where({condition})'

    @_generative
    def And(self):
        """
        ToDo: ....
        :return:
        """
        self._operation = 'Intersection'
        self._repr += f'.And()'

    @_generative
    def Or(self):
        """
        ToDo: ....
        :return:
        """
        self._repr += f'.Or()'

    @_generative
    def Not(self):
        """
        ToDo: ....
        :return:
        """
        self._repr += f'.Not()'

    @_generative
    def between(self, low, high, low_open=False, high_open=False):
        """
        ToDo:...
        scalar operations with an interval
        :param low: lower limit point
        :param high: upper limit point
        :param low_open:
        :param high_open:
                closed interval (default) ---> low_open=False, high_open=False
                open interval ---> low_open=True, high_open=True
                half open interval ---> low_open=False, high_open=True
                half open interval ---> low_open=True, high_open=False
        :return: BooleanArray Mask that is used in filter()
        """
        self._repr += f'.between({low}, {high})'
        raise InvalidPipeOperation(f'Operator _between_ is not implemented')

    @_generative
    def In(self):
        """
        ToDo:.....
        1st case comma separated string or list of string values
        e.g. 'Fairfax Village, Anacostia Metro, Thomas Circle, 15th & Crystal Dr'
             ('Fairfax Village', 'Anacostia Metro', 'Thomas Circle', '15th & Crystal Dr')

        2nd case list of numeric values
        e.g. (31706, 31801, 31241, 31003)
        """
        self._repr += f'.In()'
        raise InvalidPipeOperation(f'Operator _in_ is not implemented')

    @_generative
    def like(self, pattern):
        """
        Notice: like operator can also be used in where() as a string
        :param str pattern: match substring in column string values
        :return: PyArrow Boolean Array mask (self.fetch) that is used in filter()
                 it also returns boolean mask to calls from ASETPipe.where(), ASETPipe.And() methods
        """
        self.fetch = pc.match_substring(self.fetch.cast(self._vtype), pattern)
        self._operation = 'Restriction'
        self._repr += f'.like({pattern})'

    @_generative
    def filter(self, mask=None):
        """
        It uses a boolean array mask (self.fetch) constructed in previous chained operation to filter
        HACOL data represented with a DictionaryArray

        :param mask: this is used when we call filter() externally from ASETPipe.filter() method
                          to update the filtering state of HACOL
        :return: DictionaryArray, i.e. HACOL.data filtered
                 the filtered DictionaryArray is pointed at self._hacol.filtered_data
        """
        if self._operation != 'Restriction' and mask is None:
            raise InvalidPipeOperation(f'Apply a condition first with filtering operators')

        if mask:
            self.fetch = mask

        # Update HACOL.filtered_data
        # @filtered_data.setter is filtering HACOL data with a mask (self.fetch)
        self._hacol.filtered_data = self.fetch

        # Return the filtered DictionaryArray
        self.fetch = self._hacol.filtered_data

        # Alter the filtered state
        self._hacol.hatoms_included = len(self.fetch.indices.unique())
        self._hacol.values_included = len(self.fetch.indices)
        self._hacol.filtered = True
        self._operation = 'Filtering'
        self._repr += f'.filter()'

    @_generative
    def slice(self, limit=None, offset=0):
        """
        slice is used either to limit the number of entries to return in the states dictionary
        or to limit the members of HyperAtom collection, i.e. hyperatoms (values)

        :param limit:  number of records to return from the result set
        :param offset: number of records to skip from the result set
        :return: A slice of records
        """
        if self._operation == 'Dictionary':
            if limit:
                self.fetch = self.fetch[offset:limit]
            else:
                self.fetch = self.fetch[offset:]
        else:
            self.fetch = self.fetch.slice(offset=offset, length=limit)
        self._repr += f'.slice({limit}, {offset})'
        if not self._operation:
            self._operation = 'Slicing'

    @_generative
    def to_array(self, order=None, unique=False):
        """
        :param order: default None, 'asc', 'desc'
        :param unique: take distinct elements in array
        :return: by default PyArrow Array or PyArrow DictionaryArray if dictionary=False
        """
        # Valid is the case that self.fetch is not empty, i.e. there are non-null values
        if self.fetch.is_valid():
            # Transform
            self.fetch = DSUtils.pyarrow_dict_to_arr(self.fetch)
            if unique:
                self.fetch = self.fetch.unique()
            # Sort
            if order == 'asc':
                self.fetch = DSUtils.pyarrow_sort(self.fetch, ascending=True)
            elif order == 'desc':
                self.fetch = DSUtils.pyarrow_sort(self.fetch, ascending=False)
        else:
            # In the case filtering did not return any values, return an empty array,
            self.fetch = pa.array([], self._vtype)

        self._repr += f'.to_array()'
        self._operation = 'Transformation'

    @_generative
    def to_numpy(self, order=None, limit=None, offset=0):
        """
        :param order: default None, 'asc', 'desc'
        :param limit:  number of values to return from HACOL
        :param offset: number of values to skip from HACOL
        :return:
        """
        # Valid is the case that self.fetch is not empty, i.e. there are non-null values
        if self.fetch.is_valid():
            # Transform
            self.fetch = DSUtils.pyarrow_to_numpy(self.fetch)
            # Sort
            if order == 'asc':
                self.fetch = np.sort(self.fetch)
            elif order == 'desc':
                self.fetch = np.sort(self.fetch)[::-1]

            if limit:
                self.fetch = self.fetch[offset:limit]
            else:
                self.fetch = self.fetch[offset:]
        else:
            # In the case filtering did not return any values, return an empty array,
            self.fetch = np.empty(0)

        self._repr += f'.to_numpy()'
        self._operation = 'Transformation'

    @_generative
    def to_series(self, order=None, limit=None, offset=0):
        """
        :param order: default None, 'asc', 'desc'
        :param limit:  number of values to return from HACOL
        :param offset: number of values to skip from HACOL
        :return: Pandas Series
        """
        # Valid is the case that self.fetch is not empty, i.e. there are non-null values
        if self.fetch.is_valid():
            # Sort PyArrow Array
            if order:
                self.fetch = DSUtils.pyarrow_dict_to_arr(self.fetch)
                # Sort
                if order == 'asc':
                    self.fetch = DSUtils.pyarrow_sort(self.fetch, ascending=True)
                elif order == 'desc':
                    self.fetch = DSUtils.pyarrow_sort(self.fetch, ascending=False)
            # Transform
            self.fetch = self.fetch.to_pandas()

            if limit:
                self.fetch = self.fetch.iloc[offset:limit]
            else:
                self.fetch = self.fetch.iloc[offset:]
        else:
            # In the case filtering did not return any values, return an empty array,
            self.fetch = Series([], dtype=object)

        self._repr += f'.to_series()'
        self._operation = 'Transformation'

    @_generative
    def to_hyperlinks(self, hb2=10001):
        """
        :param hb2: dim2 value for hyperbonds,
                    it is set at a high enough value >10000 to filter them later on in the graph of data
        :return: HyperLinks (edges that connect a HyperBond with HyperAtoms)
                 List of pairs in the form [ ((hb2, hb1), (ha2, ha1)), ((hb2, hb1), (ha2, ha1)), ...]
                 These are used to create a data graph
        """
        # Get the indices of filtered data column
        col_indices = self.fetch.indices

        # Create a mask to exclude null values
        mask = col_indices.is_valid()

        # HyperAtoms
        ha1arr = col_indices.filter(mask)

        # HyperBonds of the filtered data set (these are not the same with those at unfiltered state)
        pk = pa.array(np.arange(self._hacol.values_included))
        hb1arr = pk.filter(mask)

        # HyperLinks
        self.fetch = zip(zip_with_scalar(hb2, hb1arr.to_numpy()),
                         zip_with_scalar(self._hacol.dim2, ha1arr.to_numpy()))

        self._repr += f'.to_hyperlinks()'
        self._operation = 'Transformation'

    @_generative
    def to_string_array(self):
        """
        :return: List of string values
                 This is a string representation for the valid (non-null) values of the filtered HACOL
                 It is used in the construction of a data graph to set the `value` property of the node
        """
        # Transform PyArrow DictionaryArray to Array
        mask = self.fetch.indices.is_valid()
        val_array = DSUtils.pyarrow_dict_to_arr(self.fetch)

        # String representation of values in the filtered column
        self.fetch = val_array.filter(mask).to_string()[4:-2].split(',\n  ')

        self._repr += f'.to_string_array()'
        self._operation = 'Transformation'

    def out(self, lazy=False):
        """
        We distinguish between two cases, eager vs lazy evaluation.
        This is particularly useful when we deal with very large HyperAtom collections that do not fit in memory

        :param :lazy:

        :return: use `out()` method at the end of the chained generative methods to return the output
                 displayed with the appropriate specified format and structure
        """
        #
        # return a generator result, i.e. lazy=True
        #
        if isinstance(self.fetch, GeneratorType) and lazy:
            # ToDo lazy evaluation....
            pass
        elif self._operation in oplist:
            return self.fetch
        else:
            raise InvalidPipeOperation(f'Operation {self._operation} is not defined')


# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
# ***************************************************************************************
#                           =======   End of Module =======
# ***************************************************************************************
# \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
