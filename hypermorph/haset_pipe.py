
import pyarrow as pa
import numpy as np
from types import GeneratorType
from .utils import GenerativeBase, _generative, DSUtils, split_comma_string
from .exceptions import InvalidPipeOperation

rel_operators = ['>=', '<=', '<', '>', '=', '!=', 'like']
oplist = ['MemoryUsage', 'Counting', 'Selection', 'Restriction', 'Filtering',
          'Transformation', 'Projection', 'Slicing']


# ===========================================================================================
# Operations in a pipeline for Associative Entity Set (ASET)
# ===========================================================================================
class ASETPipe(GenerativeBase):
    def __init__(self, aset, result=None):
        """
        :param aset: Associative Entity Set (ASET)
        :param result:
        """
        self.fetch = result

        # Composition object
        self._aset = aset

        # ASETPipe object dunder representation method (__repr__)
        # that is modified according to the chaining of operations
        self._repr = ''

        # This instance variable controls the type of operation that is permitted to be executed
        # i.e. call the Execute() method and process the queries, prepare the result for output
        self._operation = None

        # Filtering Mode:
        # 1. Normal - Filter only associations (rows) of an Entity (table)
        #             using restrict conditions from one or more HACOLs (Attributes)
        #
        # e.g. acat.q.where('price<20').And('quantity>=200').filter()

        # 2. Associative - Filter associations and update the frequency, selection, including states of HACOL
        #                  using restrict conditions from one or more HACOLs (Attributes)
        #
        # e.g. acat.select.where('price<20').And('quantity>=200').filter()
        self._filtering_mode = 'Normal'

        # Associative Mode:
        # 1. Single - Associative filtering with a single HACOL
        # 2. Multiple - Associative filtering with selections from more than one HACOLs in consecutive order
        self._associative_mode = 'Single'

        # This is the hyperatom collection that is used in associative filtering to select values
        self._hacol = None

        # list of HACOL dim2 numbers, modified in over() operation
        # and used in transformations to_string_array(), to_hyperlinks()
        self._projection = None

    def __repr__(self):
        return f'{self._aset.type}[{self._aset.name}].pipe{self._repr}'

    @_generative
    def start(self):
        """
        This is used as the first method in a chain of other methods where we
        set the filtered/unfiltered data
        pipeline methods over(), slice(), to_record_batch(), to_records(), to_table(), to_dataframe() start here
        :return: RecordBatch either in filtered or unfiltered state
        """
        self.fetch = self._aset.filtered_data

    @_generative
    def count(self):
        """
        :return: number of hbonds (rows) in filtered/unfiltered state
        """
        if self._aset.filtered:
            self.fetch = self._aset.hbonds
        else:
            self.fetch = self._aset.num_rows

        self._operation = 'Counting'
        self._repr += f'.count()'

    @_generative
    def select(self):
        """
        Warning: DO NOT CONFUSE select() with over() operator
                 In HyperMorph select() is used as a flag to alter the state of HyperAtom collections
                 This is the associative filtering that takes place where we
                    a) Change the filtering state of HyperAtom collections
                    b) Update the selection, included states for each member of the HyperAtom collection
                From an end-user perspective that results in selecting values from a HyperAtom collection

        Notice: In associative filtering mode we use only where() restriction
                and we filter with values from a SINGLE HyperAtom collection
        :return:
        """
        self._repr += f'.select()'
        self._operation = 'Selection'
        self._filtering_mode = 'Associative'

    def _get_hacol_from_condition(self, cond):
        """
        :param str cond: string that represents a condition in where(), _and_, etc operators
        :return: HyperAtom collection
        """
        pos = None
        for op in rel_operators:
            if op in cond:
                pos = cond.find(op)
                break
        if pos:
            if '$' in cond:
                # get attribute dim2 as an integer
                var = int(cond[1:pos])
            else:
                # get attribute name
                var = cond[:pos].strip()
        else:
            if '$' in cond:
                # get attribute dim2 as an integer
                var = int(cond[1:])
            else:
                var = cond.strip()

        # var is either a number, dimension (dim2) of attribute or a string (attribute name)
        if isinstance(var, str):
            hacol = getattr(self._aset, var)
        elif isinstance(var, int):
            hacol = self._aset.hacols[var]
        else:
            raise InvalidPipeOperation(f'Failed to parse condition {cond}')

        return hacol

    @_generative
    def where(self, condition):
        """
        Notice: The minimum condition you specify is the attribute name or attribute dim2 dimension
        Valid conditions:
        '$2', 'quantity', 'price>=4', 'size = 10'

        :param condition:
        :return: BooleanArray Mask that is used in filter()
        """
        # parse the condition string and get HyperAtom collection
        self._hacol = self._get_hacol_from_condition(condition)

        # A new mask (self.fetch) is returned from HACOL.where()
        self.fetch = self._hacol.q.where(condition=condition).out()

        # In case where() returned a mask update ASET mask
        if self.fetch:
            # @mask.setter is using np.logical_and() to combine the two masks, the old one with the new one
            self._aset.mask = self.fetch
        # Otherwise initialize (ALL True mask)
        else:
            self.fetch = self._aset.mask

        self._operation = 'Restriction'
        self._repr += f'.where({condition})'

    @_generative
    def And(self, condition):
        """
        :param condition:
        :return: BooleanArray Mask that is used in filter()
        """
        # parse the condition string and get HyperAtom collection
        hacol = self._get_hacol_from_condition(condition)

        # A new mask (self.fetch) is returned from HACOL.where()
        self.fetch = hacol.q.where(condition=condition).out()

        # In case where() returned a mask update ASET mask
        if self.fetch:
            # @mask.setter is using np.logical_and() to combine the two masks, the old one with the new one
            self._aset.mask = self.fetch

        self._operation = 'Restriction'
        self._repr += f'.where({condition})'

    @_generative
    def filter(self):
        """
        :return:
        """
        # Filter ASET data with the mask constructed from previous filtering operations
        self.fetch = self._aset.filtered_data

        # Associative filtering
        if self._filtering_mode == 'Associative':
            # indices of the selected values from the column of a filtered table
            col_pos = self._aset.dim2_to_col_pos[self._hacol.dim2]
            filtered_column = self.fetch.column(col_pos)
            indices = filtered_column.indices.unique()
            # Update the selection state of HACOL
            self._hacol.update_select_state(indices)

            # Update included state for ALL HyperAtom collections of ASET
            # For each filtered column in RecordBatch
            for flt_col in self.fetch.columns:
                dim2 = int(flt_col._name[1:])
                indices = flt_col.indices
                hacol = self._aset.hacols[dim2]
                hacol.update_frequency_include_color_state(indices)

        # Update ASET filtered state
        self._aset.hbonds = self.fetch.num_rows
        self._aset.filtered = True
        self._operation = 'Filtering'
        self._repr += f'.filter()'

    @_generative
    def over(self, select=None, as_names=None, as_types=None):
        """
        Notice: over(), i.e. projection is chained after the filter() method

        :param select: projection over the selected metadata columns
        :param as_names: list of column names to use for resulting dataframe
                List of user-specified column names, these are used:
                i) to rename columns (SQL `as` operator)
                ii) to extend the result set with calculated columns from an expression
        :param as_types: list of data types or comma separated string of data types
        :return: RecordBatch
        """
        # Convert cname, alias column names to a list of Attribute dim2
        self._projection = self._aset.entity.get_attributes().over('dim2').take(select=select).to_tuples().out()
        batch_arrays = [self.fetch.column(self._aset.dim2_to_col_pos[dim2]) for dim2 in self._projection]
        # Other user specified column names
        if as_names:
            column_names = split_comma_string(as_names)
        else:
            column_names = split_comma_string(select)

        # There is not a rename() columns method in RecordBatch....
        self.fetch = pa.RecordBatch.from_arrays(batch_arrays, column_names)

        self._repr += f'.over({select}, {as_names}, {as_types})'
        self._operation = 'Projection'

    @_generative
    def slice(self, limit=None, offset=0):
        """
        :param limit:  number of records to return from the result set
        :param offset: number of records to skip from the result set
        :return: A slice of records
        """
        self.fetch = self.fetch.slice(offset=offset, length=limit)
        self._repr += f'.slice({limit}, {offset})'
        if not self._operation:
            self._operation = 'Slicing'

    @_generative
    def to_record_batch(self):
        """
        :return: PyArrow RecordBatch but columns are not dictionary encoded
        Notice: Always decode PyArrow RecordBatch before sending it to Pandas DataFrame, it is a lot faster
        """
        batch_arrays = [DSUtils.pyarrow_dict_to_arr(col) for col in self.fetch.columns]
        self.fetch = pa.RecordBatch.from_arrays(batch_arrays, self.fetch.schema.names)
        self._repr += f'to_table()'
        self._operation = 'Transformation'

    @_generative
    def to_records(self):
        """
        :return: NumPy Records
        """
        batch_arrays = [DSUtils.pyarrow_dict_to_arr(col) for col in self.fetch.columns]
        self.fetch = np.core.records.fromarrays(batch_arrays, names=self.fetch.schema.names)
        self._repr += f'to_records()'
        self._operation = 'Transformation'

    @_generative
    def to_table(self):
        """
        :return: PyArrow Table
        """
        self.fetch = DSUtils.pyarrow_record_batch_to_table(self.fetch)
        self._repr += f'to_table()'
        self._operation = 'Transformation'

    @_generative
    def to_dataframe(self, index=None, order_by=None, ascending=None, limit=None, offset=0):
        """
        Notice1: Use to_record_batch() transformation before chaining it to Pandas DataFrame,
                it is a lot faster this way because it decodes PyArrow RecordBatch,
                i.e. RecordBatch columns are not dictionary encoded

        Notice2: sorting (order_by, ascending) and slicing (limit, offset) in a Pandas dataframe is slow
                 but sorting has not been implemented in PyArrow and that is why we pass these parameters here

        :param order_by: str or list of str
                         Name or list of names to sort by
        :param ascending: bool or list of bool, default True the sorting order
        :param limit:  number of records to return from the result set
        :param offset: number of records to skip from the result set
        :param index: list (or comma separated string) of column names to include in pandas dataframe index
        :return: Pandas dataframe
        """
        # PyArrow Transformation
        self.fetch = self.fetch.to_pandas()

        if index:
            self.fetch.set_index(split_comma_string(index), inplace=True)

        if order_by:
            self.fetch.sort_values(by=split_comma_string(order_by),
                                   ascending=split_comma_string(ascending), inplace=True)

        if limit:
            self.fetch = self.fetch.iloc[offset:limit]
        else:
            self.fetch = self.fetch.iloc[offset:]

        self._repr += f'to_dataframe({index})'
        self._operation = 'Transformation'

    @_generative
    def to_hyperlinks(self):
        """
        :return: HyperLinks (edges that connect a HyperBond with HyperAtoms)
                 List of pairs in the form [ ((hb2, hb1), (ha2, ha1)), ((hb2, hb1), (ha2, ha1)), ...]
                 These are used to create a data graph
        Notice: Set HACOLs to filtered state first,
                using self._aset.update_hacols_filtered_state()
        """
        hlinks = []
        for dim2 in self._projection:
            hacol = self._aset.hacols[dim2]
            hlinks.extend(hacol.q.to_hyperlinks().out())

        self.fetch = hlinks
        self._repr += f'.to_hyperlinks()'
        self._operation = 'Transformation'

    @_generative
    def to_string_array(self, unique=False):
        """
        :param unique:
        :return: List of string values
         This is a string representation for the valid (non-null) values of the filtered HACOL
         It is used in the construction of a data graph to set the `value` property of the node

        Notice: Set HACOLs to filtered state first,
                using self._aset.update_hacols_filtered_state()
        """
        values = []
        for dim2 in self._projection:
            hacol = self._aset.hacols[dim2]
            values.extend(hacol.q.to_string_array().out())

        if unique:
            # We used pyarrow unique() to maintain the order as it finds unique values
            # this is used in GData.add_values() method
            self.fetch = pa.array(values).unique().to_numpy(zero_copy_only=False).astype(str)
        else:
            self.fetch = np.array(values, dtype='str')

        self._repr += f'.to_string_array()'
        self._operation = 'Transformation'

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
        if isinstance(self.fetch, GeneratorType) and lazy:
            return self.fetch
        elif self._operation in oplist:
            return self.fetch
        else:
            raise InvalidPipeOperation(f'Operation {self._operation} is not defined')

# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
# ***************************************************************************************
#                           =======   End of Module =======
# ***************************************************************************************
# \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
