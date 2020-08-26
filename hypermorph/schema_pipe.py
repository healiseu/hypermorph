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
import pandas as pd
import numpy as np

from types import GeneratorType
from collections import Sequence

from . import vp_names, calculated_properties, field_meta, conn_meta
from .exceptions import InvalidPipeOperation
from .draw_hypergraph import IHyperGraphPlotter
from .utils import split_comma_string, GenerativeBase, _generative


# ===========================================================================================
# Pipe for Schema operations
# ===========================================================================================
class SchemaPipe(GenerativeBase):
    """
        Implements method chaining:
        A query operation, e.g. projection, counting, filtering
        can invoke multiple method calls. Each method corresponds to a query operator such as:
        get_components.over().to_dataframe().out()

        `out()` method is always at the end of the chained generative methods to return the final result

        Each one of these operators returns an intermediate result `self.fetch`
        allowing the calls to be chained together in a single statement.

        SchemaPipe methods such as get_*(), are wrapped inside methods of derivative classes of Schema, SchemaNode
        so that when they are called from these methods the result can be chained to other methods of SchemaPipe
        In that way we implement easily and intuitively transformations and conversion to multiple output formats

        Notice: we distinguish between two different execution types according to the evaluation of the result
            i) Lazy evaluation, see for example to_***() methods
            ii) Eager evaluation
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

        # Default projection
        self._project = 'nid, dim4, dim3, dim2, cname, alias, ntype, ctype, counter'.split(', ')

        # The type of operation that is permitted to be executed
        self._operation = 'UNDEFINED'

        # SchemaPipe object dunder representation method (__repr__)
        # that is modified according to the chaining of operations
        self._repr = ''

    def __repr__(self):
        return f'{self._node}.pipe{self._repr}'

    @property
    def schema_node(self):
        return self._node

    @_generative
    def filter(self, value=None, attribute=None, operator='eq', reset=True):
        """
        Notice1: to create a filtered Graph from a list/array of nodes
                that is a result of previous operations in a pipeline
                leave attribute=None, value=None
                to create a Graph from a list/array of nodes that is a result
                from the execution of other Python commands leave attribute=None and set value=[set of nodes]

        :param attribute: is a defined vertex property (node attribute)
                          for filtering vertices of the graph (Schema nodes),
        :param value: the value of the attribute to filter vertices of the graph
        :param operator: e.g. comparison operator for the values of node
        :param reset: set the Graph in unfiltered state then filter, otherwise it's a composite filtering
        :return: pass self.fetch to the next chainable operation
        """
        self._repr += f'.filter({value}, {attribute}, {operator}, {reset})'
        if attribute is None and value is None:
            # Filter with a list of node ids from a previous SchemaPipe operation where self.fetch points at
            self._schema.set_filter(filter_value=self.fetch, reset=reset)
        elif attribute is None and value is not None:
            self._schema.set_filter(filter_value=value, reset=reset)
        elif attribute is not None and value is not None:
            self._schema.set_filter(filter_attribute=attribute, filter_value=value, operator=operator, reset=reset)
        self.fetch = self._schema.vids

    @_generative
    def filter_view(self, value=None, attribute=None, operator='eq', reset=True):
        """
        Notice1: to create a GraphView from a list/array of nodes
                that is a result of previous SchemaPipe operations
                leave attribute=None, value=None
                to create a GraphView from a list/array of nodes that is a result
                from the execution of other Python commands leave attribute=None and set value=[set of nodes]

        :param attribute: is a defined vertex property (node attribute)
                          for filtering vertices of the graph (Schema nodes) to create a GraphView,
        :param value: the value of the attribute to filter vertices of the graph
        :param operator: e.g. comparison operator for the values of node
        :param reset: set the GraphView in unfiltered state, otherwise it's a composite filtering
        :return: pass self.fetch to the next chainable operation
        """
        self._repr += f'.filter_view({value}, {attribute}, {operator}, {reset})'
        if attribute is None and value is None:
            # Filter with a list of node ids from a previous SchemaPipe operation where self.fetch points at
            self._schema.set_filter_view(filter_value=self.fetch, reset=reset)
        elif attribute is None and value is not None:
            self._schema.set_filter_view(filter_value=value, reset=reset)
        elif attribute is not None and value is not None:
            self._schema.set_filter_view(filter_attribute=attribute, filter_value=value, operator=operator, reset=reset)
        self.fetch = self._schema.vids_view

    @_generative
    def take(self, select, key_column='cname'):
        """
        Take specific nodes from the result of get_*() methods
        :param select: list of integers (node IDs) or
                       list of strings (cname(s), alias(es))
                Notice: all selected nodes specified must exist otherwise it will raise an exception
        :param key_column: e.g. cname, alias
        :return: a subset of numpy array with node IDs

        Notice the difference:
            over() is a projection over the selected metadata columns (e.g. nid, dim3, dim2,...)
            take() is a projection over the selected fields of a database table, flatfile (e.g. npi, city, state,...)

        Example:
        mis.get(414).get_fields().over('nid, dim3, dim2, cname')
                                 .take(select='npi, pacID, profID, city, state').to_dataframe('dim3, dim2').out()
        """
        self._repr += f'.take({select})'
        if isinstance(select, str):
            select = select.split(', ')

        if all(isinstance(x, str) for x in select):
            # Convert list of cname(s) or list of alias(es) to list of node ids
            d = self.to_dict(key_column=key_column, value_columns='nid').out()
            try:
                select = [d[node] for node in select]
            except KeyError as ke:
                raise InvalidPipeOperation(f'Failed to take node with {key_column}={ke}')
        elif all(isinstance(x, int) for x in select):
            pass
        else:
            raise InvalidPipeOperation(f'Failed to take nodes with select={select}')

        try:
            # search all the node IDs in `select` parameter and find the indices
            indices = np.searchsorted(self.fetch, select)
            # if index is out of bounds, i.e. nodeID is not found, it will raise an IndexError exception
            self.fetch = np.take(self.fetch, indices)
        except IndexError:
            raise InvalidPipeOperation(f'Failed to take nodes with select={select}')

    @_generative
    def get_all_nodes(self):
        """
        sets Graph or GraphView to the unfiltered state
        :return: all the nodes of the Graph or all the nodes of the GraphView
        """
        self._repr += f'.get_all_nodes()'
        self.fetch = self._schema.vids

    @_generative
    def get_components(self):
        """
        Get node IDs for the components of a specific DataModel (Entity, Attribute) or DataSet (Table, Field, ....)
        It creates a filtered GraphView of the Schema for nodes that have dim3=SchemaNode.dim3

        :return: self.fetch point to Entity, Attribute, Table, Field, GraphDataModel, GraphSchema nodes
                 these node ids are passed to the next chainable operation
        """
        self._repr += f'.get_components()'
        if self._node.ntype == 'DS' or self._node.ntype == 'DM':
            # Set filtering mode on GraphView
            self._schema.set_filter_view(filter_attribute='dim3', filter_value=self.schema_node.dim3)
        # Fetch node IDs from the filtered Schema graph (GraphView)
        self.fetch = self._schema.vids_view

    @_generative
    def get_overview(self):
        """
        Get an overview of systems, datasets, datamodels, etc by filtering Schema nodes that have dim2=0
        :return: self.fetch point to the set of filtered node ids, these are passed to the next chainable operation
        """
        self._repr += f'.get_overview()'
        # Set filtering mode on GraphView
        self._schema.set_filter_view(filter_attribute='dim2', filter_value=0)
        # Fetch node IDs from the filtered Schema graph (GraphView)
        self.fetch = self._schema.vids_view

    @_generative
    def get_systems(self):
        """
        Get System node IDs including the root system
        :return: self.fetch points to the set of System node ids, these are passed to the next chainable operation
        """
        self._repr += f'.get_systems()'
        self.fetch = np.append([0], self._schema.root.out_nids)

    @_generative
    def get_datasets(self):
        """
        Get DataSet node IDs of data resources system (drs)
        :return: self.fetch points to the set of DataSet node ids, these are passed to the next chainable operation
        """
        self._repr += f'.get_datasets()'
        self.fetch = self._schema.drs.out_nids

    @_generative
    def get_datamodels(self):
        """
        Get DataModel node IDs of data model system (dms)
        :return: self.fetch points to the set of DataModel node ids, these are passed to the next chainable operation
        """
        self._repr += f'.get_datamodels()'
        self.fetch = self._schema.dms.out_nids

    @_generative
    def get_tables(self):
        """
        Get Table node IDs of a DataSet
        :return: self.fetch points to the set of Table node ids, these are passed to the next chainable operation
        """
        self._repr += f'.get_tables()'
        self.fetch = self._node.out_nids

    @_generative
    def get_fields(self, mapped=None):
        """
        Wrapped in Table(SchemaNode) class
        Get Field node IDs of a Table or Field node IDs of a DataSet
        :param mapped: if True return ONLY those fields that are mapped onto attributes
                       default return all fields
        :return: self.fetch points to the set of Field node ids, these are passed to the next chainable operation
        """
        self._repr += f'.get_fields(mapped={mapped})'
        # Fetch Field objects (nodes)
        if self._node.ntype == 'TBL':
            self.fetch = self._node.out_nodes
        elif self._node.ntype == 'DS':
            self.fetch = [n for tbl in self._node.out_nodes for n in tbl.out_nodes]

        # From those nodes check the mapped parameter, and return those that `mapped` attribute is True
        if mapped is True:
            self.fetch = [fld.nid for fld in self.fetch if fld.attributes]
        elif mapped is False:
            self.fetch = [fld.nid for fld in self.fetch if not fld.attributes]
        else:
            self.fetch = [fld.nid for fld in self.fetch]

    @_generative
    def get_graph_datamodels(self):
        """
        Get graph datamodel node ids
        :return: self.fetch that points to these node IDs
        """
        self._repr += f'.get_graph_datamodels()'
        self.fetch = self._node.out_nids

    @_generative
    def get_graph_schemata(self):
        """
        Get graph schemata node ids
        :return: self.fetch that points to these node IDs
        """
        self._repr += f'.get_graph_schemata()'
        self.fetch = self._node.out_nids

    @_generative
    def get_entities(self):
        """
        Get Entity node IDs of a DataModel or Entity node IDs of an Attribute
        :return: self.fetch point to Entity nodes these nodes are passed to the next chainable operation
        """
        self._repr += f'.get_entities()'
        if self._node.ntype == 'DM':
            self.fetch = [node.nid for node in self._node.out_nodes if node.ntype == 'ENT']
        elif self._node.ntype == 'ATTR':
            self.fetch = [obj.nid for obj in self._node.in_nodes if obj.ntype == 'ENT']

    @_generative
    def get_attributes(self, junction=None):
        """
        :param junction: if True fetch those that are junction nodes else fetch non-junction attributes
        :return: Attribute node ids of an Entity or Attribute node ids of a DataModel
        """
        self._repr += f'.get_attributes()'
        # Fetch Attribute nodes
        if self._node.ntype == 'DM':
            self.fetch = [node for node in self._node.out_nodes if node.ntype == 'ATTR']
        elif self._node.ntype == 'ENT':
            self.fetch = self._node.out_nodes

        # From those nodes check the junction parameter
        if junction is True or junction is False:
            self.fetch = [attr.nid for attr in self.fetch if attr.junction == junction]
        else:
            self.fetch = [attr.nid for attr in self.fetch]

    @_generative
    def over(self, select=None):
        """
        :param select: projection over the selected metadata columns
        :return: modifies self._project
        """
        self._repr += f'.over({select})'

        # check select parameter
        if isinstance(select, str):
            # override the default `self._project` instance variable of the node
            self._project = select.split(', ')
        elif isinstance(select, Sequence):
            self._project = select
        elif select is None:
            pass
        else:
            raise InvalidPipeOperation(f'Failed with parameter select={select}')

        # check metadata columns (projection)
        for col in self._project:
            if col not in vp_names \
                    and col not in calculated_properties \
                    and col not in field_meta \
                    and col not in conn_meta:
                raise InvalidPipeOperation(f'Failed with parameter columns={self._project}')

    @_generative
    def to_nodes(self, lazy=False):
        self._repr += f'.to_nodes()'
        if lazy:
            self.fetch = (self._schema.get(nid) for nid in self.fetch)
        else:
            self.fetch = [self._schema.get(nid) for nid in self.fetch]

    @_generative
    def to_fields(self):
        """
        converts a list of Attribute objects to a list of Field objects
        :return: list of fields that are mapped onto an Attribute
        """
        self._repr += f'.to_fields()'
        fields_nested_list = [attribute.fields for attribute in self.fetch]
        self.fetch = [f.nid for fields in fields_nested_list for f in fields]

    @_generative
    def to_nids(self, lazy=False, array=True):
        self._repr += f'.to_nids()'
        if lazy:
            self.fetch = (self._schema.get(nid).nid for nid in self.fetch)
        elif array:
            # Notice: you can get a numpy array directly,
            # without to_nids(), by chaining out() at the end of SchemaPipe operations
            pass    # self.fetch points to a numpy array data structure
        else:
            self.fetch = [self._schema.get(nid).nid for nid in self.fetch]

    @_generative
    def to_keys(self, lazy=False):
        self._repr += f'.to_keys()'
        if lazy:
            self.fetch = (tuple(self._schema.get(nid).get_value(col) for col in ['dim4', 'dim3', 'dim2'])
                          for nid in self.fetch)
        else:
            self.fetch = [tuple(self._schema.get(nid).get_value(col) for col in ['dim4', 'dim3', 'dim2'])
                          for nid in self.fetch]

    @_generative
    def to_vertices(self, lazy=False):
        self._repr += f'.to_vertices()'
        if lazy:
            self.fetch = (self._schema.get(nid).vertex for nid in self.fetch)
        else:
            self.fetch = [self._schema.get(nid).vertex for nid in self.fetch]

    @_generative
    def to_tuples(self, lazy=False):
        self._repr += f'.to_tuples()'
        if lazy:
            self.fetch = (tuple(self._schema.get(nid).get_value(col) for col in self._project) for nid in self.fetch)
        else:
            self.fetch = [tuple(self._schema.get(nid).get_value(col) for col in self._project) for nid in self.fetch]
            # Corner case: projection with only one metadata column
            if len(self._project) == 1:
                self.fetch = [col[0] for col in self.fetch]

    @_generative
    def to_dict_records(self, lazy=False):
        self._repr += f'.to_dict_records()'
        if lazy:
            self.fetch = ({col: self._schema.get(nid).get_value(col) for col in self._project} for nid in self.fetch)
        else:
            self.fetch = [{col: self._schema.get(nid).get_value(col) for col in self._project} for nid in self.fetch]

    @_generative
    def to_dataframe(self, index=None):
        """
        :param index: metadata column names to use in pandas dataframe index
        :return:
        """
        self._repr += f'.to_dataframe({index})'
        data = tuple(tuple(self._schema.get(nid).get_value(col) for col in self._project) for nid in self.fetch)
        df = pd.DataFrame(data, columns=self._project)

        # Check index parameter
        if index:
            ndx = split_comma_string(index)
            self.fetch = df.set_index(ndx)
        else:
            self.fetch = df

    @_generative
    def to_dict(self, key_column, value_columns):
        """
        :param key_column: e.g. cname, alias, nid
        :param value_columns: e.g. ['cname, alias']
        :return:
        """
        self._repr += f'.to_dict({key_column}, {value_columns})'
        if isinstance(value_columns, str):
            project = value_columns.split(', ')
        elif isinstance(value_columns, Sequence):
            project = value_columns
        else:
            raise InvalidPipeOperation(f'Failed with parameter value_columns={value_columns}')

        keys, values = [], []
        for nid in self.fetch:
            obj = self._schema.get(nid)
            keys.append(obj.get_value(key_column))
            if len(project) > 1:
                values.append(tuple(obj.get_value(col) for col in project))
            else:
                values.append(obj.get_value(project[0]))

        self.fetch = dict(zip(keys, values))

    @_generative
    def to_entity(self, entity_name='NEW Entity', entity_alias='NEW_ENT', entity_description=None,
                  datamodel=None, datamodel_name='NEW DataModel', datamodel_alias='NEW_DM', datamodel_descr=None,
                  attributes=None, as_names=None, as_types=None):
        """
        Map a Table object of a DataSet onto an Entity of a DataModel, there are two scenarios:

        a) Map Table to a new Entity and selected fields (or all fields) of the table onto new attributes
            The new entity can be linked to a new datamodel (datamodel=None) or to an existing datamodel                               
        
        b) Map selected fields (or all fields) of a table onto existing attributes of a datamodel
            It's a bipartite matching of fields with attributes and there is one-to-one correspondence between
            fields and attributes. User must specify the `datamodel` parameter. 

        Notice1:
        The Field-Attribute relationship is a Many-To-One
        i.e. many fields of different Entity objects are mapped onto one (same) Attribute

        Notice2: In both (a) and (b) cases
        fields are selected with a combination of get_fields() and take() SchemaPipe operations on the table object
        
        Example for (a): 
        get(414).get_fields().take('npi, pacID, profID, last, first, gender, graduated, city, state').
                to_entity(cname='Physician', alias='Phys').out()

        Example for (b):

        :param entity_name:
        :param entity_alias:
        :param entity_description:

        :param datamodel: create a new datamodel by default or pass an existing DataModel object
        :param datamodel_name:
        :param datamodel_alias:
        :param datamodel_descr:

        :param attributes: list of integers (Attribute IDs) or
                           list of strings (Attribute cnames, aliases) of an existing Entity or
                           None (default) to create new Attributes
        :param as_names: in the case of creating new attributes, list of strings one for each new attribute
        :param as_types: in the case of creating new attributes, list of strings one for each new attribute
                         Notice: data types can be inferred later on when we use arrow dictionary encoding...

        :return: An Entity object
        """
        self._repr += f'.to_entity({entity_name}, {entity_alias}, datamodel={datamodel}, attributes={attributes}, ' \
                      f'as_names={as_names}, as_types={as_types})'

        # (a) Map Table to a NEW Entity and selected fields or all fields of the table onto NEW Attribute(s)
        # Notice that fields are selected from previous chainable operations
        # e.g. mis.get(307).get_fields().take(select_fields)
        if not attributes:
            # Create a NEW DataModel otherwise pass datamodel parameter
            # to create the NEW Entity under an existing DataModel
            if not datamodel:
                datamodel = self._schema.add(what='datamodel',
                                             cname=datamodel_name, alias=datamodel_alias, descr=datamodel_descr)
            # Create a new Entity of the DataModel
            entity = datamodel.add_entity(cname=entity_name, alias=entity_alias, descr=entity_description)
            # Get fields as a list of Field objects
            fields = self.to_nodes().out()

            # Create new Attributes of the Entity
            for fld in fields:
                attr = datamodel.add_attribute(entity.alias, **fld.descriptive_metadata)
                # link Field (from node - tail) to Attribute (to node - head), i.e. create edge
                self._schema.add_link(fld, attr, etype=2, ename=10, elabel=10, ealias=10)

            #
            # Modify attribute properties
            #
            if as_names:
                as_names = split_comma_string(as_names)
            if as_types:
                as_types = split_comma_string(as_types)

            for ndx, attr in enumerate(entity.get_attributes().to_nodes().out()):
                # Copy cname, i.e. field name of the data resource to the alias of the Attribute
                # We will need these names when we fetch data from the data resource and encode them in PyArrow Table
                # see ASET.dictionary_encode()
                if as_types:
                    # Modify vtype
                    attr.extra['vtype'] = as_types[ndx]
                if as_names:
                    # Modify cname
                    attr.schema.alias[attr.nid] = as_names[ndx]
                else:
                    attr.schema.alias[attr.nid] = attr.schema.cname[attr.nid]

            self.fetch = entity
        # (b) Map selected fields or all fields onto existing attributes
        elif attributes and datamodel:
            # Get fields as a list of Field objects
            fields = self.to_nodes().out()
            # Get attributes
            attributes = datamodel.get_attributes().take(select=attributes).to_nodes().out()
            # link Field (from node - tail) to Attribute (to node - head), i.e. create edge
            for fld, attr in zip(fields, attributes):
                # link Field (from node - tail) to Attribute (to node - head), i.e. create edge
                self._schema.add_link(fld, attr, etype=2, ename=10, elabel=10, ealias=10)
            self.fetch = datamodel
        else:
            raise InvalidPipeOperation(f'Operation {self._repr} failed')

    @_generative
    def to_hypergraph(self):
        # Wrapped in Table(SchemaNode) class
        self._repr += f'.to_hypergraph()'
        edges = []
        if self._node.ntype == 'DM':
            hedges = self.get_entities().to_nodes().out()
            hedge_labels = [he.alias for he in hedges]
            for obj in hedges:
                edges += obj.out_edges_ids

            hnodes = self.get_attributes().to_nodes().out()
            hnode_labels = [f'{n.alias}\n{n.vtype}' for n in hnodes]
            hnode_color, hedge_color = 'green', 'red'
        elif self._node.ntype == 'ENT':
            hedges = [self._node]
            hedge_labels = [self._node.alias]
            edges += self._node.out_edges_ids

            hnodes = self.get_attributes().to_nodes().out()
            hnode_labels = [f'{n.alias}\n{n.vtype}' for n in hnodes]
            hnode_color, hedge_color = 'green', 'red'

        elif self._node.ntype == 'TBL':
            hedges = [self._node]
            hedge_labels = [self._node.cname]
            edges += self._node.out_edges_ids

            hnodes = self._node.out_nodes
            hnode_labels = [f'{n.cname}\n{n.vtype}' for n in hnodes]
            hnode_color, hedge_color = 'yellow', 'cyan'
        else:
            raise InvalidPipeOperation(f'Cannot convert object {self._node} to igraph')

        # Vertex IDs
        vids = [n.nid for n in hnodes]
        # Vertex Colors
        vcolors = [hnode_color]*len(hnodes)

        # Add hnode labels
        labels_dict = {k: v for k, v in zip(vids, hnode_labels)}
        # Add hedge labels
        for ndx, he in enumerate(hedges):
            labels_dict[he.nid] = hedge_labels[ndx]

        # Add hnode colors
        colors_dict = {k: v for k, v in zip(vids, vcolors)}
        # Add hedge colors
        for he in hedges:
            colors_dict[he.nid] = hedge_color

        # Create igraph
        self.fetch = IHyperGraphPlotter(edges, labels_dict, colors_dict)

    def plot(self, **kwargs):
        """
        Graphical output to visualize hypergraphs, it is also used in out() method
        (see IHyperGraphPlotter.plot method)
        Example:
        mis.get(535).to_hypergraph().plot()
        or
        mis.get(535).to_hypergraph().out()

        :param kwargs:
        :return:
        """
        self._repr += f'.plot()'
        if self._node.ntype == 'ENT' and 'edge_label' not in kwargs:
            edge_label = 'hasATTR'
        elif self._node.ntype == 'TBL' and 'edge_label' not in kwargs:
            edge_label = 'hasFLD'
        else:
            edge_label = None

        if 'edge_label' not in kwargs:
            kwargs['edge_label'] = edge_label
        return self.fetch.plot(**kwargs)

    def out(self, **kwargs):
        """
        :return: use `out()` method at the end of the chained generative methods to return the
        output of SchemaNode objects displayed with the appropriate specified format and structure
        """
        if isinstance(self.fetch, GeneratorType):
            return self.fetch
        elif isinstance(self.fetch, IHyperGraphPlotter):
            return self.plot(**kwargs)
        else:
            return self.fetch

# ***************************************************************************************
#   ************************** End of SchemaPipe Class ********************************
# ***************************************************************************************
