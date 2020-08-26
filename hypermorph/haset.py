
import numpy as np
import pyarrow as pa
from pandas import DataFrame
from . import flat_file_types, db_types
from .schema_dms_entity import Entity
from .exceptions import ASETError
from .utils import FileUtils, MemStats, bytes2mb
from .hacol import HAtomCollection
from .haset_pipe import ASETPipe


# ===========================================================================================
# Associative Entity Set (ASET)
# ===========================================================================================
class ASET(object):
    """
    An AssociativeSet, also called AssociativeEntitySet, is ALWAYS bounded to a SINGLE entity
    An AssociativeSet is a Set of Association objects (see Association Class)
    An AssociativeSet can also be represented with a set of HyperBonds

    There is a direct analogy with the Relational model:

    Relation : A set of tuples                    ----> Associative Set : A set of Associations
    Body     : tuples of ordered values           ----> Body : Associations
    Heading  : A tuple of ordered attribute names ----> Heading : A set of attributes
    View     : Derived relation                   ----> Associative View: A derived set of Associations

    ASET is bringing together but at the same time keep them separate under the same object:
        metadata stored in an Entity of the DataModel
        data (self._data) stored in PyArrow DictionaryEncoded Table object from one or more DataSet(s)
    """

    def __init__(self, entity, debug):
        """
        :param Entity entity: is a SchemaNode object that has a mapping defined
        :param debug:
        """
        if type(entity) is not Entity:
            raise TypeError(f'Wrong type {type(entity)} for object {entity} must be Entity')

        if not entity.has_mapping():
            raise ASETError(f'Failed to initialize ASET, {entity} does not have any mapping onto its Attribute(s)')

        self._mem = MemStats()  # memory statistics

        # Initialize
        self._ent = entity
        self._type = 'ASET'         # type of hyper-structure
        self._filtered = None       # filtered state of Associative Set
        self._num_rows = 0          # number of rows in ASET
        self._hbonds = 0            # number of hyperbonds, associations (rows) included in ASET
        self._hacols = {}           # dictionary of hyperatom collections
        self._mask = None           # Boolean Array mask to filter PyArrow dictionary encoded Table
        self._data = None           # PyArrow dictionary encoded Table

        # Dictionary that converts Attribute dim2 to column position in RecordBatch (self._data)
        self.dim2_to_col_pos = None

        # set @property decorators
        self._set_public_properties()
        # Initialize self._data = PyArrow dictionary encoded Table
        data_file_location = self._get_data_file_location()
        if data_file_location.exists():
            self._load_data(file_location=data_file_location)

    def _load_data(self, file_location=None, data=None, read_record_batch=True):
        """
        :param file_location:
        :return:
        """
        # Read RecordBatch from file this is skipped when we call _load_data from dictionary_encode
        if read_record_batch:
            data = FileUtils.pyarrow_read_record_batch(file_location.as_posix())
        # Construct HACOLs
        self._hacols = self._construct_hyperatom_collections(data)
        self._set_public_properties_for_attribute_names()
        # Count number of hyperbonds, associations (rows) included in ASET in either filtered or unfiltered state
        self._num_rows = data.num_rows
        self._hbonds = self.pipe.count().out()
        # Notice the filtered state is dependent on self._hbonds and self.num_rows
        self._filtered = self.is_filtered()
        # This is a zero copy RecordBatch from dictionary arrays in HACOLs
        self._data = pa.RecordBatch.from_arrays([hacol.data for hacol in self._hacols.values()],
                                                [f'_{k}' for k in self._hacols.keys()])
        # Reset filtering state
        self.reset()

    def __repr__(self):
        return f'{self._type}({self.dim3}, {self.dim2})[{self.alias}] {self._filtered_to_str()}'

    def __str__(self):
        return f'{self._type}({self.dim3}, {self.dim2})[{self.name}] = ' \
               f'{self._hbonds} associations {self._filtered_to_str()}'

    def reset(self, hacols_only=False):
        """
        ASET reset includes:
            Construction of PyArrow Boolean Array mask with ALL True
            reset of filtered state to False
            reset of Hyperbonds
            reset of HACOLs
        :param hacols_only: Flag for partial reset of HACOLs only
        :return:
        """
        if not hacols_only:
            # Reset mask
            self._mask = pa.array(np.ones(self.num_rows).astype('bool'))
            # Reset filtering state
            self._filtered = False
            # Reset number of hyperbonds, associations (rows) included in ASET
            self._hbonds = self.num_rows

        # Reset filtering state of HACOLs
        [hacol.reset() for hacol in self._hacols.values()]

    def update_hacols_filtered_state(self):
        """
        Update the filtering state of HyperAtom collections
        This is used when we want to operate with HyperAtom collections at filtered state
        <aset>.<hacol>.<operation>

        For a single HACOL we can also use the form
        <aset>.<hacol>.q.filter(<aset.mask>).<operation>.out()
        :return:
        """
        # Update filtered state of HACOLs
        # It is used in normal filtering mode to pass the filtering of ASET to HACOLs
        [hacol.q.filter(mask=self.mask) for hacol in self.hacols.values()]

    def _construct_hyperatom_collections(self, data):
        """
        :param data: PyArrow RecordBatch
        :return: dictionary of HyperAtom Collections
        """
        d = {}
        self.dim2_to_col_pos = {int(dim2str[1:]): ndx for ndx, dim2str in enumerate(data.schema.names)}
        for attr in self._ent.get_attributes().to_nodes().out():
            d[attr.dim2] = HAtomCollection(attr, data.column(self.dim2_to_col_pos[attr.dim2]))
        return d

    def _get_data_file_location(self):
        path = FileUtils.get_full_path_parent(self._ent.schema.net_path) / f'ASETS'
        file_location = path / f'{self._ent.dim4}.{self._ent.dim3}.{self._ent.dim2}.batch'
        return file_location

    def _save_data(self, data):
        """
        :param data: PyArrow RecordBatch
        """
        where = self._get_data_file_location().as_posix()
        FileUtils.pyarrow_write_record_batch(data, where)

    def _set_public_properties_for_attribute_names(self):
        for hacol in self._hacols.values():
            setattr(self, hacol.attribute.cname, hacol)
            setattr(self, hacol.attribute.alias, hacol)

    def _set_public_properties(self):
        prop_names = ['type', 'key', 'dim4', 'dim3', 'dim2', 'alias', 'name']
        prop_values = [self._type, self._ent.key, self._ent.dim4, self._ent.dim3, self._ent.dim2,
                       self._ent.alias, self._ent.cname]
        [setattr(self, name, value) for name, value in zip(prop_names, prop_values)]

    def _filtered_to_str(self):
        if self._filtered:
            return ' < FLT > '
        else:
            return ''

    # ---------------------------------------------------------------------
    # Warning: Do not place the following property decorators in setattr()
    # ----------------------------------------------------------------------
    @property
    def entity(self):
        return self._ent

    @property
    def attributes(self):
        return self._ent.attributes

    @property
    def data(self):
        return self._data

    @property
    def filtered_data(self):
        return self._data.filter(self._mask)

    @property
    def mask(self):
        return self._mask

    @mask.setter
    def mask(self, new_mask):
        self._mask = pa.array(np.logical_and(self._mask, new_mask), type=pa.bool_())

    @property
    def hacols(self):
        return self._hacols

    @property
    def num_rows(self):
        return self._num_rows

    @property
    def hbonds(self):
        return self._hbonds

    @hbonds.setter
    def hbonds(self, count):
        self._hbonds = count

    @property
    def filtered(self):
        return self._filtered

    @filtered.setter
    def filtered(self, status):
        self._filtered = status

    def is_filtered(self):
        """
        :return: The filtered state of ASET
        """
        filtered = False
        if self._hbonds < self._num_rows:
            filtered = True
        return filtered

    @property
    def pipe(self):
        """
        Returns an ASETPipe GenerativeBase object that refers to an instance of a HyperCollection
        use this object to chain operations and to update the state of HyperCollection instance.
        """
        return ASETPipe(self)

    @property
    def q(self):
        """
        wrapper for the starting point of a query pipeline
        :return:
        """
        return self.pipe.start()

    @property
    def select(self):
        """
        wrapper for the starting point of a query pipeline in associative filtering mode
        :return:
        """
        return self.pipe.select()

    def count(self):
        """
        wrapper for ASETPipe.count() method
        :return:
        """
        return self.pipe.count()

    def print_rows(self, select=None, as_names=None,
                   index=None, order_by=None, ascending=None, limit=None, offset=None):
        """
        wrapper on three pipeline methods over(), to_record_batch(), to_dataframe()
        for printing records on display
        Example:
        phys.q.over(cname_list).to_record_batch().\
               to_dataframe(order_by='city, last, first', limit=20, index='npi, pacID').out()
        Becomes:
        phys.print_rows(select=cname_list, order_by='city, last, first', limit=20, index='npi, pacID')
        :param select:
        :param as_names:
        :param index:
        :param order_by:
        :param ascending:
        :param limit:
        :param offset:
        :return:
        """
        result = self.q.over(select=select, as_names=as_names).to_record_batch().\
            to_dataframe(order_by=order_by, index=index, limit=limit, offset=offset, ascending=ascending).out()
        return result

    def memory_usage(self, mb=True, dataframe=True):
        """
        :param mb: output units MegaBytes
        :param dataframe: flag to display output with a Pandas dataframe
        :return:
        """
        data_size = self.data.nbytes
        sdict_size = 0

        for hacol in self.hacols.values():
            sdict_size += hacol.states_dict.nbytes
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

    def dictionary_encode(self, delimiter=None, nulls=None, skip=0, trace=None):
        """
        It will load data from the DataSet, it currently supports tabular format (rows or columns of a data table)
        and will apply PyArrow DictionaryArray encoding to the columns

        :param delimiter: 1-character string specifying the boundary between fields of the record
        :param nulls: list of strings that denote nulls e.g. ['\\N']
        :param skip: number of rows to skip at the start of the flat file
        :param trace: trace execution of query, i.e. print query, ellapsed time, rows in set,  etc....
        :return: PyArrow RecordBatch constructed with DictionaryEncoded Array objects
        """
        data_pipe = None
        data_table = self._ent.get_tables()[0]

        # Check if the user has specified value types for attributes
        attribute_vtypes = [attr.vtype for attr in self._ent.get_attributes().to_nodes().out()]
        if attribute_vtypes[0] == 'NA':
            as_types = None
        else:
            as_types = attribute_vtypes

        # Data resource is a flat file 'CSV', 'TSV',
        if data_table.ctype in flat_file_types:
            select = [f'_{attr.dim2}' for attr in self._ent.get_attributes().to_nodes().out()]
            # WARNING3: PyArrow issue with the column_names parameter in ReadOptions() is reported to JIRA
            # Hack to fix this, add OMIT strings to column_names parameter, once it is fixed delete the following lines
            cnt_all_fields = len(data_table.get_fields().out())
            cnt_diff = cnt_all_fields - len(select)
            column_names = select + ['OMIT']*cnt_diff

            # Transform a Table resource (flatfile) to a PyArrow dictionary encoded table
            data_pipe = data_table.dpipe.get_rows().over(select=select, as_names=column_names, as_types=as_types)\
                .to_batch(delimiter=delimiter, nulls=nulls, skip=skip, trace=trace)
        elif data_table.ctype == 'FEATHER':
            select = [attr.cname for attr in self._ent.get_attributes().to_nodes().out()]
            as_names = [f'_{attr.dim2}' for attr in self._ent.get_attributes().to_nodes().out()]
            data_pipe = data_table.dpipe.get_columns().over(select=select, as_names=as_names).to_table().to_batch()
        elif data_table.ctype in db_types:
            # projection, i.e. selection of fields from a database table, flat file is based on the defined Attribute(s)
            # Each field name is copied to attribute.alias when we transform the HyperMorph Table with to_entity()
            select = [attr.cname for attr in self._ent.get_attributes().to_nodes().out()]
            #
            as_names = [f'_{attr.dim2}' for attr in self._ent.get_attributes().to_nodes().out()]
            data_pipe = data_table.dpipe.get_rows().over(select=select, as_names=as_names, as_types=as_types).\
                to_batch(trace=trace)

        # PyArrow RecordBatch
        aset_data = data_pipe.out()

        if attribute_vtypes[0] == 'NA':
            # Data type inference from PyArrow
            # take the value types from the dictionary encoded table and assign them to the attributes
            vtypes = [t.value_type.__str__() for t in aset_data.schema.types]
            for ndx, attr in enumerate(self._ent.get_attributes().to_nodes().out()):
                attr.extra['vtype'] = vtypes[ndx]

        # Save PyArrow RecordBatch into a file
        self._save_data(aset_data)

        # Construct HACOLs, and reset
        self._load_data(data=aset_data, read_record_batch=False)

# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
# ***************************************************************************************
#                           =======   End of Module =======
# ***************************************************************************************
# \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
