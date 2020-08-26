import numpy as np
import hypermorph.data_graph_link as gdl

from .exceptions import GraphNodeError
from enum import Enum

vp_names = ['nid', 'dim2', 'dim1', 'ntype', 'value']


class GDataNode(object):
    """
        The GDataNode class:
            a) if vid is None
                create a NEW node, i.e. a new vertex on the graph with properties
            b) if vid is not None
                initialize a node that is represented with an existing vertex with vid
    """
    def __init__(self, gdata, vid=None, **vprops):
        """
        :param gdata: object of type GData
        :param vid: vertex id of the GData graph
        :param vprops: vertex properties
        """
        self._gdata = gdata
        self._vertex = None

        # Initialize instance variables
        self.nid, self.dim2, self.dim1, self.ntype = [None]*4
        self.value = ''

        # New GDataNode case
        if vid is None:
            # Check mandatory GDataNode properties
            for prop_nam in ['dim2', 'dim1', 'ntype']:
                if prop_nam not in vprops:
                    raise GraphNodeError(f'Failed to create GraphDataNode with {vprops}, missing '
                                         f'mandatory vertex properties dim2, dim1, ntype')
            # Add and initialize optional GDataNode properties
            for prop_nam in vp_names:
                if prop_nam not in vprops and prop_nam != 'nid':
                    vprops[prop_nam] = getattr(self, prop_nam)
            #  Create a new vertex (node) in the graph with vertex properties
            self._vertex = gdata.add_vertex(**vprops)
            # Update properties of the GraphDataNode instance
            self._update_gdata_node_properties()
        # Existing GraphDataNode case
        else:
            try:
                # Fetch graph vertex
                self._vertex = gdata.graph.vertex(vid)
                # Update properties of the GraphDataNode instance
                self._update_gdata_node_properties()
            except ValueError:
                raise GraphNodeError(f'Failed to create GraphDataNode with vertex id: {vid}')

    def __repr__(self):
        return f'GDN[{self.nid}]'

    def __str__(self):
        return f'GDN[{self.nid}]:{self.key}'

    def _update_gdata_node_properties(self):
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

    def _get_node_property(self, property_name):
        """
        this is used to access values that are returned from @properties of GraphDataNode and its subclasses
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
            result = self._gdata.get_vp_value(prop_name, self._vertex)
        else:
            raise GraphNodeError(f'Failed to get node value for property {prop_name}')

        return result

    @property
    def gdata(self):
        return self._gdata

    @property
    def vertex(self):
        return self._vertex

    @property
    def all(self):
        return {col: self.get_value(col) for col in vp_names}

    @property
    def key(self):
        return self.dim2, self.dim1

    @property
    def all_nids(self):
        return self._gdata.graph.get_all_neighbors(self.vertex)

    @property
    def out_nids(self):
        return self._gdata.graph.get_out_neighbors(self.vertex)

    @property
    def in_nids(self):
        return self._gdata.graph.get_in_neighbors(self.vertex)

    @property
    def all_nodes(self):
        if self.all_nids.size == 0:
            return []
        else:
            return tuple(np.vectorize(self._gdata.get)(self.all_nids))

    @property
    def out_nodes(self):
        if self.out_nids.size == 0:
            return []
        else:
            return tuple(np.vectorize(self._gdata.get)(self.out_nids))

    @property
    def in_nodes(self):
        if self.in_nids.size == 0:
            return []
        else:
            return tuple(np.vectorize(self._gdata.get)(self.in_nids))

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
        # Return all the links in the neighbourhood of GraphDataNode object
        return tuple(gdl.GDataLink(self.gdata, self.gdata.vid[e.source()], self.gdata.vid[e.target()])
                     for e in self._vertex.all_edges())

    @property
    def out_links(self):
        # Return all the outgoing links, i.e. GraphDataNode object is the source() node
        return tuple(gdl.GDataLink(self.gdata, self.gdata.vid[e.source()], self.gdata.vid[e.target()])
                     for e in self._vertex.out_edges())

    @property
    def in_links(self):
        # Return all the incoming links, i.e. GraphDataNode object is the target() node
        return tuple(gdl.GDataLink(self.gdata, self.gdata.vid[e.source()], self.gdata.vid[e.target()])
                     for e in self._vertex.in_edges())

    @property
    def out_edges_ids(self):
        # Return all the outgoing edges of the vertex. Edge here is represented with a pair of vertex indexes
        return tuple((self.gdata.vid[e.source()], self.gdata.vid[e.target()]) for e in self._vertex.out_edges())

    @property
    def in_edges_ids(self):
        # Return all the incoming edges of the vertex. Edge here is represented with a pair of vertex indexes
        return tuple((self.gdata.vid[e.source()], self.gdata.vid[e.target()]) for e in self._vertex.in_edges())

    @property
    def all_edges_ids(self):
        # Return both incoming and outgoing edges of the vertex. Edge here is represented with a pair of vertex indexes
        return tuple((self.gdata.vid[e.source()], self.gdata.vid[e.target()]) for e in self._vertex.all_edges())
