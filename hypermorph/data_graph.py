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

from numpy import zeros_like, logical_and
from sys import modules
from graph_tool import Graph, GraphView, load_graph
from .utils import FileUtils, zip_with_scalar
from .exceptions import InvalidGetOperation, InvalidAddOperation
from .data_graph_link import GDataLink

class_dict = {0: 'HyperAtom', 1: 'HyperBond'}

# *********
# WARNING : Do not erase the following import statements, even if they are not used,
# they are needed in int_to_class() function
# *********
from .data_graph_hyperatom import HyperAtom
from .data_graph_hyperbond import HyperBond


def int_to_class(class_id):
    """
    :param class_id: (0 - 'HyperAtom') or  (1 - 'HyperBond')
    :return: a class that is used in get(), get_node_by_id() methods
    """
    return getattr(modules[__name__], class_dict[class_id])


class GData(object):
    """
        GData class represents ABox "assertion components" — a fact associated with a terminological vocabulary
        Such a fact is an instance of HB-HAs association
        ABox are TBox-compliant statements about that vocabulary
        Each instance of HB-HAs association is compliant with the model (schema) of Entity-Attributes

        GData of HyperMorph are represented with a directed graph that is based on `graph_tool` python module.
        GData is composed from DataNodes and DataEdges. Each DataEdge links two DataNodes
        and we define a direction convention from a tail DataNode to a head DataNode.

        HyperAtom, HyperBond classes are derived from DataNode class

        GData of HyperMorph is a hypergraph defined by two sets of objects (a.k.a hyper-atoms HAs & hyper-bonds HBs)
        If we have 'hyper-bonds' HB={hb1, hb2, hb3} and 'hyper-atoms' B={ha1, ha2, ha3}
        then we can make a map such as d = {hn1: (ha1, ha2), hb2: (ha2), hb3: (ha1, ha2, ha3)}
        G(HB, HA, d) is the hypergraph
    """

    def __init__(self, rebuild=False, load=False, **graph_properties):
        """
        :param rebuild: create a new graph with properties and initialize HyperMorph systems
        :param load: if True, it loads a graph from filename

        :param graph_properties:
                net_type: HyperMorph type of the graph, i.e. the scope of the graph, e.g. Data
                net_edges: directed, undirected
                net_name: load graph from a file of type (.gt) or (.graphml)
                net_alias: short graph name
                net_descr: description of graph
                net_path: path to load graph from filename (net_name) and extension (.gt) or (.graphml)
                net_format: The format is guessed from file_name extension,
                            or can be specified by file_format, which can be either “gt”, “graphml”,
                net_tool: python tool that is used to create, process and save/load graph
        """
        # Graph instance
        self._graph = None

        # boolean vertex property which is used as a mask for filtering the graph, either the Graph or GraphView
        self._vmask = None

        # This is modified to a GraphView instance in filtering operations
        # GraphView is a filtered view of the Graph,
        # in that case the state of the Graph is not affected by the filtering operation,
        # i.e. after filtering Graph has the same nodes and edges as before filtering
        self._graph_view = None

        # flag for the filtered or unfiltered state of the GData
        self._is_filtered = False

        # flag for the filtered or unfiltered state of the GData GraphView
        # if GraphView is in unfiltered state then it's a mirror image of Data Graph (not a copy)
        self._is_view_filtered = False

        if rebuild:
            self._rebuild(**graph_properties)

        if load:
            self._load_graph(**graph_properties)

    def __repr__(self):
        return f'GData:{self.graph}'

    def save_graph(self):
        """
        Save HyperMorph GData._graph using the self._net_name, self._net_path and self._net_format
        """
        # Construct Graph filename
        filename = self.net_name
        # if the user specified a path then
        if self.net_path != '.':
            filename = FileUtils.get_full_path_filename(self.net_path, filename)
        filename = filename + '.' + self.net_format

        # Save the graph in a file
        self._graph.save(filename)
        return filename

    def _rebuild(self, create_vertex_properties=False, **graph_properties):
        """
        :param create_vertex_properties
                we can skip creation of vertex properties and add them later with add_hyperlinks() method
                it will be faster to populate graph with vertices and edges this way
        :param graph_properties:
        :return:
        """
        # Create Graph instance
        self._graph = Graph(directed=True)
        # Set the vertices mask for filtering graph
        self._vmask = self._graph.new_vertex_property('bool')
        # Rebuild properties
        self._create_graph_properties(**graph_properties)
        # Create Vertex Properties
        if create_vertex_properties:
            self._create_vertex_properties()

    def _load_graph(self, **graph_properties):
        # Construct Graph filename
        filename = graph_properties['net_name']
        # if the user specified a path then
        filepath = graph_properties['net_path']
        if filepath != '.':
            filename = FileUtils.get_full_path_filename(filepath, filename)
        filename = filename + '.' + graph_properties['net_format']

        # Load Graph from file
        self._graph = load_graph(filename)
        # Set the vertices mask for filtering graph
        self._vmask = self._graph.new_vertex_property('bool')

    def _create_vertex_properties(self):
        """
        Create Vertex properties (self._graph.vp.*)
        :return:
        """
        # We create nodes of two types
        #   HA (HyperAtom - 0) or HB (HyperBond - 1)
        self._graph.vp.ntype = self._graph.new_vertex_property('int')

        # String formatted value of the object
        self._graph.vp.value = self._graph.new_vertex_property('string')

        # Node dimensions
        self._graph.vp.dim2 = self._graph.new_vertex_property('int')
        self._graph.vp.dim1 = self._graph.new_vertex_property('int')

    def _create_graph_properties(self,
                                 net_type='Data', net_edges='directed', net_name='HyperMorph Graph Data',
                                 net_alias='dgraph', net_descr='Graph HyperMorph References and Values',
                                 net_path='.', net_format='graphml', net_tool='graph_tool'):
        """
        :param net_type: HyperMorph type of the graph, i.e. the scope of the graph, e.g. Data
        :param net_edges: directed, undirected
        :param net_name: name of the graph
        :param net_alias: short graph name, this is used in filename to load the graph
        :param net_descr: description of graph
        :param net_path: path to load graph from filename (net_name) and extension (.gt) or (.graphml)
        :param net_format: either “gt”, “graphml”,
                            `.net_format` is added to `net_alias` as the extension of the graph filename
        :param net_tool: python tool that is used to create, process and save/load graph
        :return:

        Any created property map can be made “internal” to the corresponding graph.
        This means that it will be copied and saved to a file together with the graph.
        Properties are internalized by including them in the graph’s dictionary-like attributes

        A property can be accessed with dictionary keys or with dot notation e.g. for `_dim3` vertex property:
        self._graph._dim3 or self._graph.vp[_dim3]
        """

        # Internalize Graph properties (self._graph.gp.*)
        self._graph.gp.net_type = self._graph.new_graph_property('string', net_type)
        self._graph.gp.net_edges = self._graph.new_graph_property('string', net_edges)
        self._graph.gp.net_name = self._graph.new_graph_property('string', net_name)
        self._graph.gp.net_alias = self._graph.new_graph_property('string', net_alias)
        self._graph.gp.net_descr = self._graph.new_graph_property('string', net_descr)
        self._graph.gp.net_path = self._graph.new_graph_property('string', net_path)
        self._graph.gp.net_format = self._graph.new_graph_property('string', net_format)
        self._graph.gp.net_tool = self._graph.new_graph_property('string', net_tool)

    @property
    def list_properties(self):
        return self._graph.list_properties()

    @property
    def vertex_properties(self):
        return self._graph.vertex_properties

    @property
    def graph_properties(self):
        return self._graph.graph_properties

    # Vertex @property decorators return:
    # VertexPropertyMap objects with a value type for the GData graph object
    # These are used in GDataNode to read metadata property values for specific GDataNode instances
    # Use the vertex_properties method to return a  dictionary of Vertex properties
    # or the properties method to list all properties of the graph
    @property
    def dim2(self):
        return self._graph.vp.dim2

    @property
    def dim1(self):
        return self._graph.vp.dim1

    @property
    def value(self):
        return self._graph.vp.value

    @property
    def ntype(self):
        return self._graph.vp.ntype

    #
    # Other Vertex @property decorators that are not saved with the Graph
    #
    @property
    def vmask(self):
        return self._vmask

    @property
    def vid(self):
        return self._graph.vertex_index

    #
    # Graph @property decorators return values of GraphPropertyMap keys
    #
    @property
    def net_type(self):
        return self._graph.gp.net_type

    @property
    def net_edges(self):
        return self._graph.gp.net_edges

    @property
    def net_name(self):
        return self._graph.gp.net_name

    @property
    def net_alias(self):
        return self._graph.gp.net_alias

    @property
    def net_descr(self):
        return self._graph.gp.net_descr

    @property
    def net_path(self):
        return self._graph.gp.net_path

    @property
    def net_format(self):
        return self._graph.gp.net_format

    @property
    def net_tool(self):
        return self._graph.gp.net_tool

    #
    # Other @property decorators of GData object
    #
    @property
    def graph(self):
        return self._graph

    @property
    def graph_view(self):
        return self._graph_view

    @property
    def is_filtered(self):
        return self._is_filtered

    @property
    def is_view_filtered(self):
        return self._is_view_filtered

    @property
    def vids(self):
        return self._graph.get_vertices()

    @property
    def vids_view(self):
        return self._graph_view.get_vertices()

    @property
    def vertices(self):
        return tuple(self._graph.vertices())

    @property
    def vertices_view(self):
        return tuple(self._graph_view.vertices())

    def set_filter(self, vmask, inverted=False):
        """
        This is filtering DGraph._graph instance
        Only the vertices with value different than False are kept in the filtered graph

        :param vmask: boolean mask for the vertices of the graph
        :param inverted: if it is set to TRUE only the vertices with value FALSE are kept.
        :return: the filtered state of the graph
        """
        self._vmask.a = vmask
        self._graph.set_vertex_filter(prop=self._vmask, inverted=inverted)
        # Set Filtered state
        self._is_filtered = True
        return self.is_filtered

    def unset_filter(self):
        """
        Reset the filtering of the DGraph._graph instance
        :return: the filtered state
        """
        self._vmask.a = zeros_like(self._vmask.a)  # clear the mask
        self._graph.set_vertex_filter(prop=None)
        self._is_filtered = False
        return self.is_filtered

    def set_filter_view(self, vmask):
        """
        DGraph._graph_view is a filtered view of the DGraph._graph,
        in that case the state of the DGraph is not affected by the filtering operation,
        i.e. after filtering DGraph._graph has the same vertices and edges as before filtering
        :param vmask: boolean mask for the vertices of the graph
        :return: filtered state of the graph view
        """
        # Create filtered GraphView
        self._vmask.a = vmask
        self._graph_view = GraphView(g=self._graph_view, vfilt=self._vmask)
        # Set Filtered state
        self._is_view_filtered = True
        return self._is_view_filtered

    def unset_filter_view(self):
        # clear the vertex mask
        self._vmask.a = zeros_like(self._vmask.a)  # clear the mask
        # Start in unfiltered state, GraphView is a mirror image of Graph
        self._graph_view = GraphView(self.graph, vfilt=None)
        # Unfiltered state
        self._is_view_filtered = False
        return self._is_view_filtered

    def at(self, dim2, dim1):
        """
        :param dim2: ha2 dimension of hyperatom or hb2 dimension of hyperbond
        :param dim1: ha1 dimension of hyperatom or hb1 dimension of hyperbond
        :return: the node of the graph with the specific dimensions
        """
        return self.get_node_by_key(dim2=dim2, dim1=dim1)

    def get(self, nid):
        """
        :param nid: Node ID (vertex id)
        :return: GDataNode object
        """
        return self.get_node_by_id(nid)

    def get_node_by_id(self, nid):
        """
        :param nid: node ID (vertex id)
        :return: GDataNode object from the derived class, i.e. HyperAtom, HyperBond object
                 see class_dict
        """
        try:
            # If the vertex does not exist it will raise an exception
            vertex_obj = self.graph.vertex(nid)
            node_type = self.ntype[nid]
            node_class = int_to_class(node_type)
            return node_class(gdata=self, vid=nid)
        except ValueError:
            raise InvalidGetOperation(f'Failed to get node with id={nid}')

    def get_node_by_key(self, dim2, dim1):
        """
        :param dim2:
        :param dim1:
        :return: object with the specific key
        """
        # Find the node ID of the object with dimensions dim2, dim1
        try:
            vmask = logical_and(self.dim2.a == dim2, self.dim1.a == dim1)
            self.set_filter(vmask)
            node_id = self.vids[0]
            self.unset_filter()
        except KeyError:
            raise InvalidGetOperation(f'Failed to get node with key {dim2, dim1}')

        # Return the object using GData.get() method
        try:
            return self.get(node_id)
        except InvalidGetOperation:
            raise InvalidGetOperation(f'Failed to get node with key={dim2, dim1}')

    def get_vp(self, vp_name):
        """
        :param vp_name: vertex property name
        :return: VertexPropertyMap object
        """
        # special case for `nid` (node/vertex id)
        if vp_name == 'nid':
            return self._graph.vertex_index
        else:
            return self.vertex_properties[vp_name]

    def get_vp_value(self, vp_name, vid):
        """
        :param vp_name: vertex property name
        :param vid: either vertex object or vertex index (node id)
        :return: the value of vertex property on the specific vertex of the graph
        """
        return self.get_vp(vp_name)[vid]

    def get_vp_values(self, vp_name, filtered=False):
        # special case for `nid`
        if vp_name == 'nid':
            return self.vids

        # all other properties
        attr = self.vertex_properties[vp_name]
        python_vtype = attr.python_value_type()
        if python_vtype == str:
            return attr.get_2d_array([0])[0]
        elif python_vtype == int:
            if filtered:
                return attr.fa
            else:
                return attr.a

    def add_vertices(self, n):
        v = self._graph.add_vertex(n)
        return v

    def add_vertex(self, **vprops):
        """
        Used in GDataNode to create a new instance of a node
        :param vprops: GData vertex properties
        :return: a vertex of GData Graph
        """
        #  node in the hypergraph is represented with a vertex of the graph
        v = self._graph.add_vertex()

        # Add vertex properties
        for key, val in vprops.items():
            self.vertex_properties[key][v] = val

        return v

    def add_edge(self, from_vertex, to_vertex):
        """
        Used in GDataLink to create a new instance of an edge
        :param from_vertex: tail vertex
        :param to_vertex: head vertex
        :return: an edge of GData Graph
        """
        # a hyperlink in the hypergraph is represented with an edge of the graph
        e = self._graph.add_edge(from_vertex, to_vertex)

        return e

    def add_link(self, from_node, to_node):
        """
        :param from_node: tail node is a  GDataNode object or node ID
        :param to_node: head node is a GDataNode object or node ID

        If there isn't a link from node, to node it will try to create a new one,
        otherwise it will  return an existing GDataLink instance

        :return: GDataLink object, i.e. an edge of the GData graph
        """
        gdata_link = GDataLink(gdata=self, from_node=from_node, to_node=to_node)
        return gdata_link

    def add_node(self, **nprops):
        """
        :param nprops: GData node (vertex) properties
        :return: HyperBond object
        """
        ntype = nprops.get('ntype')
        try:
            if ntype == 1:
                # Create new HyperBond
                graph_node = HyperBond(self, **nprops)
            elif ntype == 0:
                # Create new HyperAtom
                graph_node = HyperAtom(self, **nprops)
            else:
                raise InvalidAddOperation(f'Wrong node type, failed to create object, with ntype={ntype}')
        except Exception:
            raise InvalidAddOperation(f'Failed to create HyperBond object')

        return graph_node

    def add_values(self, string_values, hb2=10000):
        """
        Create and set a value vertex property
        :param hb2: dim2 value for hyperbonds,
                    it is set at a high enough value to filter them later on in the graph of data
        :param string_values: string_repr string representation of ha2 column UNIQUE data values
                              with a NumPy array of dtype=str
        :return:
        """
        # Filter the graph to leave only hyperatom nodes
        self._vmask.a = self.dim2.a < hb2
        self._graph.set_vertex_filter(self._vmask)

        # Create value vertex property
        self._graph.vp.value = self._graph.new_vertex_property('string', string_values, '')

        # Filter again the graph to leave only hyperbond nodes
        self._vmask.a = self.dim2.a > hb2
        self._graph.set_vertex_filter(self._vmask)

        # Set value for hyperbond nodes
        for v in self.graph.get_vertices():
            self._graph.vp.value[v] = f'HB[{self._graph.vp.dim1[v]}]'

        # Unset filter
        self._graph.set_vertex_filter(prop=None)

    def add_hyperlinks(self, hlinks, hb2=10000):
        """
        Method updates the GData graph with vertices (nodes) and edges (links) that are related to parameter hlinks
        It also adds dim2, dim1 and ntype vertex properties

        :param hb2: dim2 value for hyperbonds,
                    it is set at a high enough value to filter them later on in the graph of data
        :param hlinks is a list of hyperlinks in the form [ ((hb2, hb1), (ha2, ha1)), ....., ((hb2, hb1), (ha2, ha1))]
        A hyperlink is defined as the edge that connect a HyperBond with a HyperAtom, i.e.
        HB(hb2, hb1) ---> HA(ha2, ha1)

        In a table of data (hb2) with pk=hb1 is associated (linked) to a column of data (ha2) with indices (ha1)
        hb2 uint16>10000 represents a data table
        hb1 uint32 represents a data table row or pk index
        ha2 uint16<10000 represents a column of the data table
        ha1 uint32 represents a unique value, secondary index value of the specific column (ha2)

        Therefore the set of hyperlinks (HBi ---> HA1, HBi ---> HA2, HBi ---> HAn)
        transforms the tuple of a Relation to an association between the table row and the column values, indices

        This association is graphically represented on a hypergraph
        with a hyperedge (HB) that connects many hypernodes (HAs)
        """
        # Polulate GData graph with vertices and edges that are related to the hyperlinks
        # vp_hlink is <VertexPropertyMap object with value type 'python::object'
        vp_hlink = self._graph.add_edge_list(edge_list=list(hlinks), hashed=True)

        # For each vertex read a tuple of dimensions from the vertex property vp_hlink
        # then the list of hlink tuples is unfolded into two lists with the zip(*...)
        # these two lists are used to initialize dimensions dim2, dim1
        dim2_values, dim1_values = list(zip(*[vp_hlink[v] for v in self._graph.get_vertices()]))

        # Create node dimensions and initialize them
        self._graph.vp.dim2 = self._graph.new_vertex_property('int', dim2_values)
        self._graph.vp.dim1 = self._graph.new_vertex_property('int', dim1_values)

        # Initial values for the node type
        ntype_values = self._graph.vp.dim2.a > hb2

        # Create node type and initialize
        #   HA (HyperAtom - 0) HB (HyperBond - 1)
        self._graph.vp.ntype = self._graph.new_vertex_property('int', ntype_values)
