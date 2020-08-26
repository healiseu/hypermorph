

import sys
from . import out_types, what_pipe_types, what_types
from .haset import ASET
from .utils import session_time, MemStats, get_size, PandasUtils, FileUtils
from .schema import Schema
from .exceptions import InvalidGetOperation, InvalidAddOperation


# ===========================================================================================
# Management Information System (MIS)
# ===========================================================================================
class MIS(object):
    """
    MIS is a builder pattern class based on Schema class, ....
    """
    def __init__(self, debug=0, rebuild=False, warning=True, load=False, **kwargs):
        """
        :param rebuild: set the flag only the first time that you need to create the meta-management framework
        :param warning:
        :param debug: flag to display debugging messages during execution
        """
        # memory statistics
        self._mem_stats = MemStats()

        # meta-management schema
        self._mms = None

        # data-management graph
        self._dmg = None

        # flag to display debug info
        self._dbg = debug

        if self._dbg > 0:
            print('\nHyperMorph v0.1')
            print('Operational API for information management and data transformations '
                  'on Associative Semiotic Hypergraph Development Framework')
            print('(C) 2015-2020 Athanassios I. Hatzis\n')
            print(f'Python {sys.version} on {sys.platform}')
            print(f'Session Started on ', session_time())

        # flag to rebuild or not metadata-management schema (mms)
        if rebuild:
            self.rebuild(warning=warning, **kwargs)

        # flag to load metadata-management schema (mms)
        if load:
            self.load(**kwargs)

    def __repr__(self):
        return f'MIS(\n\t{self._mms})'

    def __str__(self):
        return f'MIS(\n\t{self._mms})'

    @staticmethod
    def size_of_object(obj):
        return get_size(obj)

    @staticmethod
    def size_of_dataframe(df, deep=False):
        return PandasUtils.dataframe_memory_usage(df, deep)

    @property
    def mem(self):
        return self._mem_stats

    @property
    def mms(self):
        return self._mms

    @property
    def root(self):
        return self._mms.root

    @property
    def drs(self):
        return self._mms.drs

    @property
    def dms(self):
        return self._mms.dms

    @property
    def hls(self):
        return self._mms.hls

    @property
    def sls(self):
        return self._mms.sls

    @property
    def overview(self):
        return self._mms.overview

    def get_overview(self):
        return self._mms.get_overview().filter_view()

    @property
    def all_nodes(self):
        return self._mms.all_nodes

    def get_all_nodes(self):
        return self._mms.get_all_nodes().filter_view()

    @property
    def systems(self):
        return self._mms.systems

    def get_systems(self):
        return self._mms.get_systems().filter_view()

    @property
    def datasets(self):
        return self._mms.datasets

    def get_datasets(self):
        return self._mms.get_datasets().filter_view()

    @property
    def datamodels(self):
        return self._mms.datamodels

    def get_datamodels(self):
        return self._mms.get_datamodels().filter_view()

    def rebuild(self, warning=True, **kwargs):
        # bypass warning
        if warning:
            key_pressed = self._display_warning()
        else:
            key_pressed = 'y'

        if key_pressed == 'y':
            print('\n\nPlease wait, rebuilding metadata graph schema ...')
            # Create new Schema instance
            self._mms = Schema(rebuild=True, **kwargs)
            print('\n Done.')
        elif key_pressed == 'n':
            print('\nOperation aborted.')

    def load(self, **kwargs):
        # key_pressed = self._display_warning()
        key_pressed = 'y'

        if key_pressed == 'y':
            print('\n\nPlease wait, loading metadata graph schema ...')
            # Create new Schema instance
            self._mms = Schema(load=True, **kwargs)
            print('\n Done.')
        elif key_pressed == 'n':
            print('\nOperation aborted.')

    @staticmethod
    def _display_warning():
        print('\n\n************************************************************************************')
        print(f'   *** WARNING: ALL METADATA IN HyperMorph Graph WILL BE LOST ***')
        print(f'***********************************************************************************')
        while True:
            query = input('Do you want to proceed ?')
            key_pressed = query[0].lower()
            if query == '' or key_pressed not in ['y', 'n']:
                print('Please answer with (y)es or (n)o !')
            else:
                break

        return key_pressed

    '''
    ###############################################################################################################
                            <----------------- Wrapper Methods ---------------> 
    ###############################################################################################################
    '''
    @staticmethod
    def _delete_aset_data_file(ent):
        """
        :param ent: Entity object
        :return:
        """
        path = FileUtils.get_full_path_parent(ent.schema.net_path) / f'ASETS'
        file_location = path / f'{ent.dim4}.{ent.dim3}.{ent.dim2}.batch'
        if file_location.exists():
            file_location.unlink()

    def save(self):
        return self._mms.save_graph()

    @staticmethod
    def add_aset(from_table=None, with_fields=None,
                 entity=None, entity_name=None, entity_alias=None, entity_description=None,
                 datamodel=None, datamodel_name='NEW Data Model',  datamodel_alias='NEW_DM', datamodel_descr=None,
                 attributes=None, as_names=None, as_types=None, debug=0):
        """
            There are three ways to create an ASET object:
                1) From an Entity that has already a mapping defined (entity)
                   fields are mapped onto the attributes of an existing Entity
                2) From a Table of a dataset (from_table, with_fields)
                   that are mapped onto the attributes of a NEW Entity that is created in an existing DataModel,
                3) From a Table of a dataset (from_table, with fields)
                   that are mapped onto the attributes of a NEW Entity that is created in a NEW DataModel
            Case (2) and (3) define a new mapping between a data set and a data model

        :param from_table:
        :param with_fields:
        :param entity:
        :param entity_name:
        :param entity_alias:
        :param entity_description:
        :param datamodel:
        :param datamodel_name:
        :param datamodel_alias:
        :param datamodel_descr:
        :param attributes:
        :param as_names:
        :param as_types:
        :param debug:

        :return:
        """
        if entity:
            # ToDo Case1: .....
            pass
        elif from_table and datamodel:
            pipe_res = from_table.get_fields()
            if with_fields:
                pipe_res = pipe_res.take(with_fields)
            entity = pipe_res.to_entity(entity_name=entity_name, entity_alias=entity_alias,
                                        entity_description=entity_description, datamodel=datamodel,
                                        as_names=as_names, as_types=as_types).out()
            # Erase previous feather file, leftover, in ASETS folder
            MIS._delete_aset_data_file(entity)
        elif from_table:
            # Case 3
            pipe_res = from_table.get_fields()
            if with_fields:
                pipe_res = pipe_res.take(with_fields)
            entity = pipe_res.to_entity(entity_name=entity_name, entity_alias=entity_alias,
                                        entity_description=entity_description, datamodel_name=datamodel_name,
                                        datamodel_alias=datamodel_alias, datamodel_descr=datamodel_descr,
                                        as_names=as_names, as_types=as_types).out()
            # Erase previous feather file, leftover, in ASETS folder
            MIS._delete_aset_data_file(entity)
        else:
            raise InvalidAddOperation(f'Failed to add Associative Entity Set')

        return ASET(entity, debug=debug)

    def add(self, what, **kwargs):
        """
        Add new nodes to HyperMorph Schema or an Associative Entity Set
        :param what: the type of node to add (datamodel, entity, entities, attribute, dataset)
        :param kwargs: pass keyword arguments to Schema.add() method
        :return: the object(s) that were added to HyperMorph Schema
        """
        if what == 'aset':
            if self._dbg:
                kwargs['debug'] = self._dbg
            return self.add_aset(**kwargs)
        else:
            return self._mms.add(what=what, **kwargs)

    def at(self, *args):
        return self._mms.at(*args)

    def get(self, nid, what='node', select=None, index=None, out='dataframe', junction=None, mapped=None,
            key_column='nid', value_columns='cname', filter_attribute=None, filter_value=None, reset=False):
        """
        This method implements the functional paradigm, it is basically a wrapper of chainable methods, for example:
        get(461).
        get_entities().
        over(select='nid, dim3, dim2, cname, alias, descr').
        to_dataframe(index='dim3, dim2').
        out()

        can be written as
        get(461, what='entities', select='nid, dim3, dim2, cname, alias, descr', out='dataframe', index='dim3, dim2')

        :param nid:
        :param what:
        :param select:
        :param index:
        :param out:
        :param junction:
        :param mapped:
        :param key_column:
        :param value_columns:
        :param filter_attribute:
        :param filter_value:
        :param reset:
        :return:
        """
        result = None
        if what not in what_types + what_pipe_types:
            raise InvalidGetOperation(f'Failed to get data with parameter what={what}')

        if what == 'node':
            result = self._mms.get(nid)
        # node metadata
        elif what == 'all_metadata':
            result = self._mms.get(nid).all
        elif what == 'field_metadata':
            result = self._mms.get(nid).metadata
        elif what == 'descriptive_metadata':
            result = self._mms.get(nid).descriptive_metadata
        elif what == 'connection_metadata':
            result = self.mms.get(nid).connection_metadata
        elif what == 'system_metadata':
            result = self.mms.get(nid).system_metadata
        # data resource container metadata
        elif what == 'container_metadata':
            kwargs = {'fields': select, 'out': out}
            result = self.mms.get(nid).container_metadata(**kwargs)
        # node's components metadata
        elif what == 'components':
            result = self._mms.get(nid).get_components().filter_view()
        elif what == 'tables':
            result = self._mms.get(nid).get_tables()
        elif what == 'fields':
            result = self._mms.get(nid).get_fields(mapped=mapped)
        elif what == 'graph_datamodels':
            result = self._mms.get(nid).get_graph_datamodels()
        elif what == 'graph_schemata':
            result = self._mms.get(nid).get_graph_schemata()
        elif what == 'entities':
            result = self._mms.get(nid).get_entities()
        elif what == 'attributes':
            result = self._mms.get(nid).get_attributes(junction=junction)

        # node data
        elif what == 'rows':
            result = self._mms.get(nid).get_rows()

        if what in what_pipe_types:
            if filter_attribute and filter_value:
                result = result.filter_view(attribute=filter_attribute, value=filter_value, reset=reset)

            if select:
                result = result.over(select=select)

            if out not in out_types:
                raise InvalidGetOperation(f'Failed to get data with parameter out={out}')

            if out == 'dataframe':
                result = result.to_dataframe(index)
            elif out == 'nodes':
                result = result.to_nodes()
            elif out == 'vertices':
                result = result.to_vertices()
            elif out == 'nids':
                result = result.to_nids()
            elif out == 'keys':
                result = result.to_keys()
            elif out == 'tuples':
                result = result.to_tuples()
            elif out == 'arrays':
                result = result.out()
            elif out == 'dict_records':
                result = result.to_dict_records()
            elif out == 'dict':
                result = result.to_dict(key_column=key_column, value_columns=value_columns)

        if what in what_pipe_types:
            return result.out()
        else:
            return result

# ***************************************************************************************
# ************************** End of MIS Class ***********************
# ***************************************************************************************


# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
# ***************************************************************************************
#                 =======   End of HyperMorph MIS Module =======
# ***************************************************************************************
# \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
