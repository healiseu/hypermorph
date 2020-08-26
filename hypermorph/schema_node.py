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
import numpy as np
import hypermorph.schema_link as tsl

from . import vp_names, field_meta, conn_meta, descr_meta, sys_meta, calculated_properties, file_types, flat_file_types
from .utils import FileUtils
# SchemaPipe, DataPipe are called from derived classed and from property methods of the Schema class
from .schema_pipe import SchemaPipe
from .data_pipe import DataPipe

from .exceptions import GraphNodeError


class SchemaNode(object):
    """
        The SchemaNode class:
            a) if vid is None
                create a NEW node, i.e. a new vertex on the graph with properties
            b) if vid is not None
                initialize a node that is represented with an existing vertex with vid

        Notice: All properties and methods defined here are accessible from derived classes
        Attribute, Entity, DataModel, DataSet, Table, Field
    """
    def __init__(self, schema, vid=None, **vprops):
        """
        :param schema: object of type Schema
        :param vid: vertex id of the Schema graph
        :param vprops: vertex properties
        """
        self._schema = schema
        self._vertex = None

        self.project = None    # projection, i.e. selection of metadata attributes

        # Initialize instance variables
        self.nid, self.counter, self.dim4, self.dim3, self.dim2 = [None]*5
        self.ntype, self.ctype, self.cname, self.alias, self.descr = ['NA']*5

        # connection metadata properties, i.e. keys of `extra` property dictionary
        self.path, self.db, self.host, self.port, self.user, self.password, self.api = ['NA']*7

        # field metadata properties, i.e. keys of `extra` property dictionary
        self.enum, self.missing, self.unique, self.junction = [0]*4
        self.vtype, self.default, self.collation = ['NA']*3

        # extra attributes dictionary
        self.extra = {}

        # New SchemaNode case
        if vid is None:
            # Check mandatory SchemaLink properties
            for prop_nam in ['counter', 'dim4', 'dim3', 'dim2', 'ntype', 'cname']:
                if prop_nam not in vprops:
                    raise GraphNodeError(f'Failed to create SchemaNode with {vprops}, missing '
                                         f'mandatory vertex properties dim4, dim3, dim2, ntype, cname, counter')
            # Add and initialize optional SchemaLink properties
            for prop_nam in vp_names:
                if prop_nam not in vprops and prop_nam != 'nid':
                    vprops[prop_nam] = getattr(self, prop_nam)
            #  Create a new vertex (node) in the graph with vertex properties
            self._vertex = schema.add_vertex(**vprops)
            # Update properties of the SchemaNode instance
            self._update_schema_node_properties()
        # Existing SchemaNode case
        else:
            try:
                # Fetch graph vertex
                self._vertex = schema.graph.vertex(vid)
                # Update properties of the SchemaNode instance
                self._update_schema_node_properties()
            except ValueError:
                raise GraphNodeError(f'Failed to create SchemaNode with vertex id: {vid}')

    def __repr__(self):
        return f'SN[{self.nid}]'

    def __str__(self):
        return f'SN[{self.nid}]:{self.key}'

    def _update_schema_node_properties(self):
        """
            Update:
                a) vertex properties dynamically as properties of the node
                b) property values for the `extra` property of the node
            `setattr` here will enable public access of these properties
            this way we avoid to write @property decorators for each one of them
        :return:
        """
        # Update vertex properties
        [setattr(self, vp_name, self.get_value(vp_name)) for vp_name in vp_names]

        # Update `extra` properties and convert database values according to the container type (ctype)....
        for val_attrib in field_meta + conn_meta:
            #
            # If the attribute has been passed as a key-value pair in the `extra` dictionary parameter
            #
            if val_attrib in self.extra:
                # Lookup value in extra dictionary
                dict_val = self.extra[val_attrib]
                # change relative path to full path
                if val_attrib == 'path' and self.ntype == 'DS' and self.ctype in flat_file_types:
                    dict_val = FileUtils.get_full_path_parent(self.schema.net_path) / f'FlatFiles/{dict_val}'
                if val_attrib == 'path' and self.ntype == 'DS' and self.ctype == 'SQLite':
                    dict_val = FileUtils.get_full_path_parent(self.schema.net_path) / f'SQLite/{dict_val}'
                if val_attrib == 'path' and self.ntype == 'DS' and self.ctype == 'FEATHER':
                    dict_val = FileUtils.get_full_path_parent(self.schema.net_path) / f'Feather/{dict_val}'
                if val_attrib == 'path' and self.ntype == 'DS' and self.ctype == 'SCHEMA':
                    dict_val = FileUtils.get_full_path_parent(self.schema.net_path) / f'Schemata/{dict_val}'
                if val_attrib == 'path' and self.ntype == 'DS' and (self.ctype == 'GRAPHML' or self.ctype == 'GT'):
                    dict_val = FileUtils.get_full_path_parent(self.schema.net_path) / f'DataModels/{dict_val}'

                # conversion of `default` attribute (any database)
                if val_attrib == 'default' and (dict_val == 'NULL' or dict_val is None):
                    dict_val = 'NA'
                if val_attrib == 'collation' and (dict_val == 'NULL' or dict_val is None):
                    dict_val = 'NA'
                # Conversion of SQLite column metadata
                # if self.ctype == 'SQLite':
                #     pass
                # Conversion of MySQL column metadata
                if self.ctype == 'MYSQL':
                    # MYSQL COLUMN_KEY
                    if val_attrib == 'unique' and dict_val == 'PRI':
                        dict_val = 1
                    elif val_attrib == 'unique':
                        dict_val = 0
                    # MYSQL IS_NULLABLE
                    if val_attrib == 'missing' and dict_val == 'YES':
                        dict_val = 1
                    elif val_attrib == 'missing':
                        dict_val = 0
                #
                setattr(self, val_attrib, dict_val)
            #
            # Else update `extra` dictionary with default values
            # for properties that have not been specified during the creation of a new node
            #
            else:
                self.extra.update({val_attrib: self._get_node_property(val_attrib)})

    def _get_node_property(self, property_name):
        """
        this is used to access values that are returned from @properties of SchemaNode and its subclasses
        e.g. obj.get_node_property('parent') is the same with obj.parent

        :param property_name: function name of the @property decorator
        :return:
        """
        return getattr(self, property_name)

    def get_value(self, prop_name):
        """
        :param prop_name:   Vertex property name  (vp_names)
                        or @property function name (calculated_properties)
                        or data type properties (field_meta)
        :return: the value of property for the specific node
        """
        if prop_name in vp_names:
            result = self._schema.get_vp_value(prop_name, self._vertex)
        elif prop_name in calculated_properties + field_meta + conn_meta:
            result = self._get_node_property(prop_name)
        else:
            raise GraphNodeError(f'Failed to get node value for property {prop_name}')

        return result

    @property
    def spipe(self):
        """
        Returns a ``Pipe`` (GenerativeBase object) that refers to an instance of SchemaNode
        use this object to chain operations defined in SchemaPipe class
        """
        return SchemaPipe(self)

    @property
    def dpipe(self):
        """
        Returns a ``Pipe`` (GenerativeBase object) that refers to an instance of SchemaNode
        use this object to chain operations defined in DataPipe class
        """
        return DataPipe(self)

    @property
    def schema(self):
        return self._schema

    @property
    def vertex(self):
        return self._vertex

    @property
    def all(self):
        return {col: self.get_value(col) for col in vp_names + field_meta + conn_meta}

    @property
    def descriptive_metadata(self):
        return {col: self.get_value(col) for col in descr_meta}

    @property
    def system_metadata(self):
        return {col: self.get_value(col) for col in sys_meta}

    @property
    def key(self):
        return self.dim4, self.dim3, self.dim2

    @property
    def all_nids(self):
        return self._schema.graph.get_all_neighbors(self.vertex)

    @property
    def out_nids(self):
        return self._schema.graph.get_out_neighbors(self.vertex)

    @property
    def in_nids(self):
        return self._schema.graph.get_in_neighbors(self.vertex)

    @property
    def all_nodes(self):
        if self.all_nids.size == 0:
            return []
        else:
            return tuple(np.vectorize(self._schema.get)(self.all_nids))

    @property
    def out_nodes(self):
        if self.out_nids.size == 0:
            return []
        else:
            return tuple(np.vectorize(self._schema.get)(self.out_nids))

    @property
    def in_nodes(self):
        if self.in_nids.size == 0:
            return []
        else:
            return tuple(np.vectorize(self._schema.get)(self.in_nids))

    @property
    def all_vertices(self):
        return tuple(self.vertex.all_neighbours())

    @property
    def out_vertices(self):
        return tuple(self.vertex.out_neighbours())

    @property
    def in_vertices(self):
        return tuple(self.vertex.in_neighbours())

    @property
    def all_links(self):
        # Return all the links in the neighbourhood of SchemaNode object
        return tuple(tsl.SchemaLink(self.schema, self.schema.vid[e.source()], self.schema.vid[e.target()])
                     for e in self._vertex.all_edges())

    @property
    def out_links(self):
        # Return all the outgoing links, i.e. SchemaNode object is the source() node
        return tuple(tsl.SchemaLink(self.schema, self.schema.vid[e.source()], self.schema.vid[e.target()])
                     for e in self._vertex.out_edges())

    @property
    def in_links(self):
        # Return all the incoming links, i.e. SchemaNode object is the target() node
        return tuple(tsl.SchemaLink(self.schema, self.schema.vid[e.source()], self.schema.vid[e.target()])
                     for e in self._vertex.in_edges())

    @property
    def out_edges_ids(self):
        # Return all the outgoing edges of the vertex. Edge here is represented with a pair of vertex indexes
        return tuple((self.schema.vid[e.source()], self.schema.vid[e.target()]) for e in self._vertex.out_edges())

    @property
    def in_edges_ids(self):
        # Return all the incoming edges of the vertex. Edge here is represented with a pair of vertex indexes
        return tuple((self.schema.vid[e.source()], self.schema.vid[e.target()]) for e in self._vertex.in_edges())

    @property
    def all_edges_ids(self):
        # Return both incoming and outgoing edges of the vertex. Edge here is represented with a pair of vertex indexes
        return tuple((self.schema.vid[e.source()], self.schema.vid[e.target()]) for e in self._vertex.all_edges())
