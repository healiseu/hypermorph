
import numpy as np
from pandas import DataFrame
from pyarrow import DictionaryArray

from .utils import bytes2mb, split_comma_string, PandasUtils
from .schema_dms_attribute import Attribute
from .hacol_pipe import HACOLPipe


# ===========================================================================================
# HyperAtom Collection (HACOL)
# ===========================================================================================
class HAtomCollection(object):
    """
    A HyperAtom Collection (HACOL) can be:
    1. A set of hyperatoms (HACOL_SET) that represent the domain of values for a specific attribute

    2. A multiset of hyperatoms (HACOL_BAG) that represents a column of data in a table
    Each hyperatom may appear multiple times in this collection because
    each hyperatom is linked to one or more hyperbonds (MANY-TO-MANY relationship)

    3. A set of values of a specific data type (HACOL_VAL) where
    each value is associated with a hyperatom from the set of hyperatoms (HACOL_SET) to form a KV pair.

    The set of KV pairs represents the domain of a specific attribute where:
    K is the key of hyperatom with dimensions (dim3-model, dim2-attribute, dim1-distinct value)
    V is the data type value
    
    HyperAtoms can be displayed with K, V or K:V pair

    All hyperatoms in (1), (2) and (3) have common dimensions (dim3, dim2) i.e. same model, same attribute

    HACOL is bringing together but at the same time keep them separate under the same object:
        metadata stored in an Attribute of the DataModel
        data (self._data) stored in PyArrow DictionaryEncoded Array object
        Notice: data points to a DictionaryEncoded Array object which is a column of a PyArrow Table
    """

    def __init__(self, attribute, data):
        """
        :param Attribute attribute:
        :param DictionaryArray data: represents a column of data,

        ToDo: HACOL operations, filtered state for junction Attribute(s)
        """
        if type(attribute) is not Attribute:
            raise TypeError(f'Wrong type {type(attribute)} for object {attribute} must be Attribute')

        # if type(data) != DictionaryArray:
        #    raise HACOLError(f'Failed to initialize HACOL, data must be of type pyarrow.lib.DictionaryArray')

        # Initialize
        self._attr = attribute  
        self._type = 'HACOL'                    # type of hyper-structure
        self._data = data                       # PyArrow Dictionary Array
        self._filtered_data = self._data        # PyArrow Dictionary Array filtered with a mask
        self._filtered = None                   # filtered state of HACOL

        self._unique = len(self._data.unique().dictionary)  # Number of HyperAtoms in the set (unique values)
        self._missing = self._data.null_count               # Number of values missing from the column
        self._length = len(self._data)                      # Total number of values in the column = instances + missing
        self._valid = self._length - self._missing      # Number of HyperAtom instances (non-missing values)

        self._hatoms = None         # Number of HyparAtoms included in HyperAtom set at filtered state
        self._values = None         # Total Number of Values (valid+missing) in HyperAtom collection at filtered state
        # ToDo: Selectivity

        # Construct HACOL states dictionary
        self._sdict = self._construct_states_dictionary()

        # set @property decorators
        self._set_public_properties()

        # Number of hatoms filtered, those that are still included in HACOL after a filter has been applied
        # if we are in unfiltered state then it holds:
        self._hatoms = self._unique

        # Number of values filtered, those instances of HyperAtoms that are included in HyperAtom collection
        # if we are in unfiltered state then it holds:
        self._values = self._length

        # Notice the filtered state is dependent on self._hatoms and self._unique
        self._filtered = self.is_filtered()

    def _construct_states_dictionary(self):
        """
        States dictionary is a NumPy Structured array (numpy.recarray) that stores
            1. ha1 dimension of HyperAtoms      (ha1)
            2. Frequencies in unfiltered state  (freq)
            3. unique values                    (val)
            4  Frequencies in filtered state    (cnt)
            5. selection state                  (sel)
            6. included state                   (inc)

        :return: NumPy Structured array
        """
        val_counts = self._data.value_counts()
        dictionary = val_counts.field(0).dictionary         # Unique Values
        indices = val_counts.field(0).indices
        freqs = val_counts.field(1)
        mask = indices.is_valid()                           # Boolean mask to exclude null
        valid_indices = indices.filter(mask)                # ha1 dimension of HyperAtoms
        valid_freqs = freqs.filter(mask)                    # Frequencies

        # Selection States (Selected/Not Selected) - Input States
        sel_state = np.zeros(len(dictionary), dtype='bool')
        # Included States - (Included / Not Included) - Output State
        inc_state = np.ones(len(dictionary), dtype='bool')

        # ToDo color state
        # Color States - 0 Exc      0|0 Grey,
        #                1 Sel      1|1 Green,
        #                2 Inc      0|1 White,
        #                3 SelExc   1|0 Grey with tick

        # For values NumPy does automatic type inference.... test this point....
        sd = np.core.records.fromarrays([valid_indices, valid_freqs, dictionary, valid_freqs,
                                         sel_state, inc_state], names='ha1, freq, val, cnt, sel, inc')
        return sd

    def _set_public_properties(self):
        prop_names = ['type', 'states_dict', 'attribute',
                      'key', 'dim4', 'dim3', 'dim2', 'alias', 'name', 'vtype',
                      'hatoms_unique', 'values_valid', 'values_missing', 'values_total']
        prop_values = [self._type, self._sdict, self._attr,
                       self._attr.key, self._attr.dim4, self._attr.dim3, self._attr.dim2,
                       self._attr.alias, self._attr.cname, self._attr.vtype,
                       self._unique, self._valid, self._missing, self._length]
        [setattr(self, name, value) for name, value in zip(prop_names, prop_values)]

    def _filtered_to_str(self):
        if self._filtered:
            return ' <FLT> '
        else:
            return ''

    def __repr__(self):
        return f'{self._type}({self.dim3}, {self.dim2})[{self.name}] {self._filtered_to_str()}'

    def __str__(self):
        return f'{self._type}({self.dim3}, {self.dim2})[{self.name}] = ' \
               f'{self._hatoms} hyperatoms, {self._values} values {self._filtered_to_str()}'

    @property
    def data(self):
        return self._data

    @property
    def filtered_data(self):
        return self._filtered_data

    @filtered_data.setter
    def filtered_data(self, mask):
        self._filtered_data = self._data.filter(mask)

    @property
    def hatoms_included(self):
        return self._hatoms

    @hatoms_included.setter
    def hatoms_included(self, count):
        self._hatoms = count

    @property
    def values_included(self):
        return self._values

    @values_included.setter
    def values_included(self, count):
        self._values = count

    def count(self, dataframe=True):
        return self.q.count(dataframe=dataframe).out()

    @property
    def pipe(self):
        """
        Returns a HACOLPipe GenerativeBase object that refers to an instance of a HyperCollection
        use this object to chain operations and to update the state of HyperCollection instance.
        """
        return HACOLPipe(self)

    @property
    def q(self):
        """
        wrapper for the starting point of a query pipeline
        :return:
        """
        return self.pipe.start()

    def memory_usage(self, mb=True, dataframe=True):
        data_size = self._data.nbytes
        sdict_size = self._sdict.nbytes
        total_size = data_size + sdict_size
        mem_size = [data_size, sdict_size, total_size]
        if mb:
            mem_size = list(map(bytes2mb, mem_size))
        result = mem_size

        if dataframe:
            columns = ['Memory Usage']
            index = ['Data', 'Dictionary', 'Total']
            result = DataFrame(result, columns=columns, index=index)
        return result

    def dictionary(self, columns=None, index=None, order_by=None, ascending=None, limit=None, offset=0):
        """
        :param columns: list (or comma separated string) of column names for pandas dataframe
        :param index: list (or comma separated string) of column names to include in pandas dataframe index
        :param order_by: str or list of str
                         Name or list of names to sort by
        :param ascending: bool or list of bool, default True the sorting order
        :param limit:  number of records to return from states dictionary
        :param offset: number of records to skip from states dictionary

        :return: states dictionary of HACOL
        """
        result = PandasUtils.dataframe(self._sdict, columns=columns, ndx=index)

        if order_by:
            result.sort_values(by=split_comma_string(order_by),
                               ascending=split_comma_string(ascending), inplace=True)

        if limit:
            result = result.iloc[offset:limit]
        else:
            result = result.iloc[offset:]

        return result

    def print_states(self, limit=10):
        """
        wrapper for dictionary()
        :param limit:
        :return:
        """
        return self.dictionary(limit=limit, order_by='cnt, sel, val', ascending=[False, False, True],
                               index='sel, inc, cnt, val')

    @property
    def filtered(self):
        return self._filtered

    @filtered.setter
    def filtered(self, status):
        self._filtered = status

    def is_filtered(self):
        """
        :return: The filtered state of the HACOL
        """
        filtered = False
        if self._values < self._length:
            filtered = True
        return filtered

    def reset(self):
        self._hatoms = self._unique
        self._values = self._length
        self._filtered = False
        self._filtered_data = self._data

        # Reset cnt, inc, sel states
        self._sdict.cnt = self._sdict.freq
        self._sdict.inc = np.zeros(self._unique, dtype=bool)
        self._sdict.sel = np.zeros(self._unique, dtype=bool)


    def update_select_state(self, indices):
        """
        :param indices: unique indices of the selected values (pyarrow.lib.Int32Array)
        :return:
        """
        self._sdict.sel[indices] = True

    def update_frequency_include_color_state(self, indices):
        """
        In associative filtering we update frequency, include and color state for ALL HACOLs

        :param indices: unique indices of filtered values  (pyarrow.lib.Int32Array)
                        these are values that are included in a column of a filtered table
        :return:
        """
        # Count frequencies of the indices
        val_counts = indices.value_counts()
        unique_indices = val_counts.field(0)
        frequency = val_counts.field(1)

        mask = unique_indices.is_valid()                    # Boolean mask to exclude null
        valid_indices = unique_indices.filter(mask)         # ha1 dimension of HyperAtoms
        valid_freqs = frequency.filter(mask)                # Frequencies

        # Reset cnt (frequency in filtered state)
        self._sdict.cnt = np.zeros(self._unique, dtype='uint32')
        # Update cnt state
        self._sdict.cnt[valid_indices] = valid_freqs.to_numpy()

        # Reset include state
        self._sdict.inc = np.zeros(self._unique, dtype=bool)
        # Update include state
        self._sdict.inc[valid_indices] = True

        # ToDo: Update the color state

# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
# ***************************************************************************************
#                           =======   End of Module =======
# ***************************************************************************************
# \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
