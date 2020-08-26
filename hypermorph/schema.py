
import numpy as np
from graph_tool import Graph, GraphView, load_graph
from sys import modules

from . import container_types
from .exceptions import InvalidGetOperation, InvalidAddOperation
from .utils import FileUtils
from .schema_link import SchemaLink

# *********
# WARNING : Do not erase unused import statements, they are needed in str_to_class() function
# *********
from .schema_sys import System
from .schema_dms_datamodel import DataModel
from .schema_dms_entity import Entity
from .schema_dms_attribute import Attribute
from .schema_drs_dataset import DataSet
from .schema_drs_table import Table
from .schema_drs_field import Field
from .schema_drs_graph_schema import GraphSchema
from .schema_drs_graph_datamodel import GraphDataModel

class_dict = {'SYS': 'System',
              'DS': 'DataSet', 'TBL': 'Table', 'FLD': 'Field',
              'GDM': 'GraphDataModel', 'GSH': 'GraphSchema',
              'DM': 'DataModel', 'ENT': 'Entity', 'ATTR': 'Attribute'}

eprop_dict = {'etype': {1: 'Hierarchical', 2: 'Mapping'},
              'ename': {1: 'SYS-SYS', 2: 'SYS-DS', 3: 'SYS-DM',
                        4: 'DS-TBL', 5: 'DS-GDM', 6: 'TBL-FLD',
                        7: 'DM-ENT', 8: 'DM-ATTR', 9: 'ENT-ATTR',
                        10: 'FLD-ATTR', 11: 'DS-GSH'},
              'elabel': {1: 'hasSubSystem', 2: 'hasDataSet', 3: 'hasDataModel',
                         4: 'hasTable', 5: 'hasGraphDataModel', 6: 'hasField',
                         7: 'hasEntity', 8: 'hasAttribute', 9: 'hasAttribute',
                         10: 'ontoAttribute', 11: 'hasGraphSchema'},
              'ealias': {1: 'hasSub', 2: 'hasDS', 3: 'hasDM',
                         4: 'hasTBL', 5: 'hasGDM', 6: 'hasFLD',
                         7: 'hasENT', 8: 'hasATTR', 9: 'hasATTR',
                         10: 'ontoATTR', 11: 'hasGSH'}
              }


def str_to_class(class_name):
    """
    :param class_name: e.g. Table, Entity, Attributes (see class_dict)
    :return: a class that is used in get(), get_node_by_id() methods
    """
    return getattr(modules[__name__], class_name)


class Schema(object):
    """
    Schema class creates a data catalog, i.e. meta-data repository.
    Data catalog resembles (TBox) a vocabulary of "terminological components", i.e. abstract terms
    Data catalog properties e.g. dimensions, names, counters, etc describe the concepts in a data dictionary
    These terms are Entity types, Attribute types, Data Resource types, Link(edge) types, etc....
    TBox is about types and relationships between types
    e.g. Entity-Attribute, Table-Column, Object-Fields, etc....

    Schema of HyperMorph is represented with a directed graph that is based on `graph_tool` python module.
    Schema graph is composed from SchemaNodes and SchemaEdges. Each SchemaEdge links two SchemaNodes
    and we define a direction convention from a tail SchemaNode to a head SchemaNode.

    System, DataModel, DataSet, GraphDataModel, Table, Field, classes are derived from SchemaNode class

    Schema of HyperMorph is a hypergraph defined by two sets of objects (a.k.a. hyper-nodes & hyper-edges).
    If we have 'hyper-edges' HE={he1, he2, he3} and 'hyper-nodes' B={hn1, hn2, hn3}
    then we can make a map such as d = {he1: (hn1, hn2), he2: (hn2), he3: (hn1, hn2, hn3)}
    G(HE, HN, d) is the hypergraph
    """
    def __init__(self, rebuild=False, load=False, **graph_properties):
        """
        :param rebuild: create a new graph with properties and initialize HyperMorph systems
        :param load: if True, it loads a graph from filename

        :param graph_properties:
                net_type: HyperMorph type of the graph, i.e. the scope of the graph, e.g. Schema
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

        # flag for the filtered or unfiltered state of the Schema Graph
        self._is_filtered = False

        # flag for the filtered or unfiltered state of the Schema GraphView
        # if GraphView is in unfiltered state then it's a mirror image of Schema Graph (not a copy)
        self._is_view_filtered = False

        if rebuild:
            self._rebuild(**graph_properties)

        if load:
            self._load_graph(**graph_properties)

    def __repr__(self):
        return f'Schema:{self.graph}'

    def save_graph(self):
        """
        Save HyperMorph Schema._graph using the self._net_name, self._net_path and self._net_format
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

    def _rebuild(self, **graph_properties):
        """
        :param graph_properties:
        :return:
        """
        # Create Graph instance
        self._graph = Graph(directed=True)
        # Set the vertices mask for filtering graph
        self._vmask = self._graph.new_vertex_property('bool')
        # Rebuild properties
        self._create_graph_properties(**graph_properties)
        self._create_vertex_properties()
        self._create_edge_properties()
        # Add HyperMorph root meta-management system and subsystems
        self._add_systems()

    def _create_edge_properties(self):
        """
        Create Edge properties (self._graph.ep.*)
        :return:
        """
        # edge type (see etype_dict)
        self._graph.ep.etype = self._graph.new_edge_property('short')
        # edge name (see ename_dict)
        self._graph.ep.ename = self._graph.new_edge_property('short')
        # edge label (see elabel_dict)
        self._graph.ep.elabel = self._graph.new_edge_property('short')
        # edge alias (see ealias_dict)
        self._graph.ep.ealias = self._graph.new_edge_property('short')

    def _create_vertex_properties(self):
        """
        Create Vertex properties (self._graph.vp.*)
        :return:
        """
        # node type in data catalog:
        # it is used to specify what kind of object, i.e. term concept, we store in metadata dictionary
        #   DMS(Data Model SubSystem), DM(Data Model), ENT(Entity)
        #   DRS(Data Resources SubSystem), DS(Data Set), TBL (Table), GDM (Graph data model), GSH (Graph Schema)
        #   HLS(HyperLink SubSystem), SLS(SchemaLink Subsystem)...
        #   ATTR(Attribute), FLD(Field)
        self._graph.vp.ntype = self._graph.new_vertex_property('string', val='')

        # container type:
        # it is used to specify the type of a data resource container,
        # i.e. graph files (GRPAHML, GT), flat files (CSV, TSV), database table (MYSQL),
        # IMPORTANT NOTICE:
        # It DOES NOT describe the type of content for schema object but its container that is part of
        # e.g. if we have a Field object that is part of a TSV file, the container here is the TSV file
        self._graph.vp.ctype = self._graph.new_vertex_property('string')

        # canonical name (cname) is the name that is used in the data resource, e.g. field names in flat files
        self._graph.vp.cname = self._graph.new_vertex_property('string')

        # alias is an alternate name that we assign to a Model, Entity, Attribute e.g. short name, abbreviation
        # see test_modules examples
        self._graph.vp.alias = self._graph.new_vertex_property('string')

        # Description of object
        self._graph.vp.descr = self._graph.new_vertex_property('string')

        # counter for the number of
        # Data Models, Entities, Attributes,
        # Data Resources, Tables, Columns
        # HyperLink Types, HyperLinks
        self._graph.vp.counter = self._graph.new_vertex_property('int')

        # Node dimensions
        self._graph.vp.dim4 = self._graph.new_vertex_property('int')
        self._graph.vp.dim3 = self._graph.new_vertex_property('int')
        self._graph.vp.dim2 = self._graph.new_vertex_property('int')

        # `extra` metadata vertex property
        # object here is a python dictionary with the following keys
        #
        # Field/Attribute value properties
        # < junction > indicates that Attribute is linked to more than one Entities (ENT - tail nodes)
        # < enumerated > indicates categorical type of values
        # < missing > whether to permit or not an attribute to have missing values,
        #
        # Notice: In HyperMorph 'missing' attribute replaces both 'mandatory' and 'nullable' attributes:
        # If missing is set to 1 (True) then the attribute can have missing values (SQL nullable)
        # if missing is set to 0 (False) then missing values are not permitted, i.e. it is mandatory to have a value
        #

        # < unique > no duplicates
        # < default > default value if it is not specified
        # < collation > used in ordering
        # < vtype > value type e.g. integer, string, float, etc....
        #
        # Database Connection properties
        # < database name >
        # < host >
        # < port >
        # < user >
        # < password >
        # < path > full path location of the data resources, data model on the disk, internet
        self._graph.vp.extra = self._graph.new_vertex_property('object')

        # ToDo: implement another flag to indicate whether junction is open or closed
        # if bridge=True (junction open) then you can traverse to the next entity

        # ToDo: implement another flag to indicate whether junction plays the role of primary or foreign key
        # depending on the Entity it is part of ....

        # ToDo: Labels on hyperlink types to describe direct and reverse direction (traversal path)
        # label_direct
        # label_reverse

    def _create_graph_properties(self,
                                 net_type='Schema', net_edges='directed', net_name='hypermorph_schema',
                                 net_alias='sgraph', net_descr='Schema graph for HyperMorph metadata',
                                 net_path='.', net_format='graphml', net_tool='graph_tool'):
        """
        :param net_type: HyperMorph type of the graph, i.e. the scope of the graph, e.g. Schema
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
    def edge_properties(self):
        return self._graph.edge_properties

    @property
    def graph_properties(self):
        return self._graph.graph_properties

    # Edge @property decorators return:
    # EdgePropertyMap objects with a value type for the Schema Graph object
    # These are used in SchemaLink to read metadata property values for specific SchemaLink instances
    # Use the edge_properties method to return a dictionary of Edge properties
    # or the properties method to list all properties of the graph
    @property
    def etype(self):
        return self._graph.ep.etype

    @property
    def ename(self):
        return self._graph.ep.ename

    @property
    def elabel(self):
        return self._graph.ep.elabel

    @property
    def ealias(self):
        return self._graph.ep.ealias

    # Vertex @property decorators return:
    # VertexPropertyMap objects with a value type for the Schema Graph object
    # These are used in SchemaNode to read metadata property values for specific SchemaNode instances
    # Use the vertex_properties method to return a  dictionary of Vertex properties
    # or the properties method to list all properties of the graph
    @property
    def counter(self):
        return self._graph.vp.counter

    @property
    def dim4(self):
        return self._graph.vp.dim4

    @property
    def dim3(self):
        return self._graph.vp.dim3

    @property
    def dim2(self):
        return self._graph.vp.dim2

    @property
    def cname(self):
        return self._graph.vp.cname

    @property
    def alias(self):
        return self._graph.vp.alias

    @property
    def descr(self):
        return self._graph.vp.descr

    @property
    def ctype(self):
        return self._graph.vp.ctype

    @property
    def extra(self):
        return self._graph.vp.extra

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
    # Other @property decorators of Schema Graph object
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
    def root(self):
        return System(self, 0)

    @property
    def drs(self):
        return System(self, 1)

    @property
    def dms(self):
        return System(self, 2)

    @property
    def hls(self):
        return System(self, 3)

    @property
    def sls(self):
        return System(self, 4)

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

    def set_filter(self, filter_value, filter_attribute=None, operator='eq', reset=True, inverted=False):
        """
        This is filtering the Schema Graph instance
        :param filter_value: the value of the attribute to filter vertices of the graph
                             or a list of node ids (vertex ids)
        :param filter_attribute: is a defined vertex property for filtering vertices of the graph (Schema nodes)
                                to create a GraphView
        :param operator: e.g. comparison operator for the values of node
        :param reset: set the GraphView in unfiltered state, i.e. parameter vfilt=None
                      set the vertex mask in unfiltered state, i.e. fill array with zeros
                      this step is necessary when we filter with node_ids

        :param inverted:
        :return: the filtered state
        """
        if reset:
            self.unset_filter()

        # Modify the elements of boolean valued Property map
        if filter_attribute is None:
            if reset:
                self._vmask.a = np.zeros_like(self.vmask.a)     # clear the mask
            self._vmask.a[filter_value] = 1
        elif filter_attribute and operator == 'eq':
            self._vmask = (self.get_vp_values(filter_attribute) == filter_value)

        # Create filtered Graph
        self._graph.set_vertex_filter(prop=self.vmask, inverted=inverted)
        # Set Filtered state
        self._is_filtered = True
        return self.is_filtered

    def unset_filter(self):
        """
        Reset the filtering of the Schema Graph, notice that Schema Graph
        :return: the filtered state
        """
        self._graph.set_vertex_filter(prop=None)
        self._is_filtered = False
        return self.is_filtered

    def set_filter_view(self, filter_value, filter_attribute=None, operator='eq', reset=True):
        """
        GraphView is a filtered view of the Graph,
        in that case the state of the Graph is not affected by the filtering operation,
        i.e. after filtering Graph has the same nodes and edges as before filtering

        :param filter_value: the value of the attribute to filter vertices of the graph
                             or a list of node ids (vertex ids)
        :param filter_attribute: is a defined vertex property for filtering vertices of the graph (Schema nodes)
                                to create a GraphView
        :param operator: e.g. comparison operator for the values of node
        :param reset: set the GraphView in unfiltered state, i.e. parameter vfilt=None
                      set the vertex mask in unfiltered state, i.e. fill array with zeros
                      this step is necessary when we filter with node_ids
        :return:
        """
        # If we don't reset the filtered view then we apply multiple filters one on top of the other
        if reset:
            self.unset_filter_view()
        vmask = None
        # Modify the elements of boolean valued Property map
        if filter_attribute is None:
            if reset:
                self._vmask.a = np.zeros_like(self.vmask.a)     # clear the mask
            self._vmask.a[filter_value] = 1
            vmask = self._vmask
        elif filter_attribute and operator == 'eq':
            vmask = (self.get_vp_values(filter_attribute) == filter_value)

        # Create filtered GraphView
        self._graph_view = GraphView(g=self._graph_view, vfilt=vmask)
        # Set Filtered state
        self._is_view_filtered = True

        return self._is_view_filtered

    def unset_filter_view(self):
        # Start in unfiltered state, GraphView is a mirror image of Graph
        self._graph_view = GraphView(self.graph, vfilt=None)
        # Unfiltered state
        self._is_view_filtered = False
        return self._is_view_filtered

    def at(self, dim4, dim3, dim2):
        """
        Notice: Only data model, data resource  objects have keys with dimensions (dim4, dim3, dim2)

        :param dim4: dim4 is taken from self.dms.dim4 or self.drs.dim4 it is fixed and never changes
        :param dim3: represents a datamodel or dataset object
        :param dim2: represents a component of datamodel or dataset object
        :return: the dataset  or the datamodel object with the specific key
        """
        return self.get_node_by_key(dim4=dim4, dim3=dim3, dim2=dim2)

    def get(self, nid):
        """
        :param nid: Node ID (vertex id)
        :return: SchemaNode object
        """
        return self.get_node_by_id(nid)

    def get_node_by_id(self, nid):
        """
        :param nid: node ID (vertex id)
        :return: SchemaNode object
        """
        node_type = self.ntype[nid]
        if node_type:
            node_class = str_to_class(class_dict[node_type])
            return node_class(schema=self, vid=nid)
        else:
            raise InvalidGetOperation(f'Failed to get node with id={nid}')

    def get_node_by_key(self, dim4, dim3, dim2):
        """
        Notice: Only data model, data resource  objects have keys with dimensions (dim4, dim3, dim2)

        :param dim4: dim4 is taken from self.dms.dim4 or self.drs.dim4 it is fixed and never changes
        :param dim3: represents a datamodel or dataset object
        :param dim2: represents a component of datamodel or dataset object
        :return: the dataset  or the datamodel object with the specific key
        """
        # if the object with the key is in data resources subsystem
        if dim4 == self.drs.dim4 and dim3:
            # Associate object node IDs with dim3 dimensions
            dim3_dict = {self.dim3[nid]: nid for nid in self.drs.out_nids}
        # if the object with the key is in data models subsystem
        elif dim4 == self.dms.dim4 and dim3:
            # Associate object node IDs with dim3 dimensions
            dim3_dict = {self.dim3[nid]: nid for nid in self.dms.out_nids}
        else:
            raise InvalidGetOperation(f'Failed to get node with key {dim4, dim3, dim2}, '
                                      f'use valid key for a DataSet, a DataModel or any of its components')

        # Find the node ID of the dataset or datamodel that the object is part of
        try:
            # Return the vertex id of the dataset or datamodel
            dim3_vid = dim3_dict[dim3]
        except KeyError:
            raise InvalidGetOperation(f'Failed to get node with key {dim4, dim3, dim2}')

        # Get the node ID of the object by adding dim2
        node_id = dim3_vid+dim2
        # Return the object using Schema.get() method
        try:
            return self.get(node_id)
        except InvalidGetOperation:
            raise InvalidGetOperation(f'Failed to get node with key={dim4, dim3, dim2}')

    def get_ep(self, ep_name):
        """
        :param ep_name: edge property name
        :return: EdgePropertyMap object
        """
        return self.edge_properties[ep_name]

    def get_ep_value(self, ep_name, edge):
        """
        :param ep_name: edge property name
        :param edge:
        :return: the enumerated value of edge property on the specific edge of the graph
                  the value is enumerated with a key in the eprop_dict
        """
        enum_key = self.get_ep(ep_name)[edge]
        return eprop_dict[ep_name][enum_key]

    def get_ep_values(self, ep_name):
        return self.get_ep(ep_name).a

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

    def get_all_nodes(self):
        """
        :return: result from  `get_all_nodes` method that can be chained to other operations e.g. filter_view(),
        """
        self.unset_filter()
        return self.root.spipe.get_all_nodes()

    @property
    def all_nodes(self):
        """
        :return: shortcut for SchemaPipe operation to set the GraphView in unfiltered state and get all the nodes
        """
        return self.get_all_nodes().to_dataframe(index='dim4, dim3, dim2').out()

    def get_overview(self):
        """
        :return: result from  `get_datamodels` method that can be chained to other operations e.g. over(), out()
        use out() at the end of the chained methods to retrieve the final result
        """
        self.unset_filter()
        return self.root.spipe.get_overview()

    @property
    def overview(self):
        """
        :return: shortcut for SchemaPipe operations to output an overview of systems, datamodels, datasets in a dataframe
        """
        return self.get_overview().to_dataframe(index='dim4, dim3, dim2').out()

    def get_systems(self):
        """
        :return: result from  `get_systems` method that can be chained to other operations e.g. over(), out()
        use out() at the end of the chained methods to retrieve the final result
        """
        self.unset_filter()
        return self.root.spipe.get_systems()

    @property
    def systems(self):
        """
        :return: shortcut for SchemaPipe operations to output systems metadata in a dataframe
        """
        return self.get_systems().over('nid, cname, alias, ctype, counter').to_dataframe('nid').out()

    def get_datasets(self):
        """
        :return: result from  `get_datasets` method that can be chained to other operations e.g. over(), out()
        use out() at the end of the chained methods to retrieve the final result
        """
        self.unset_filter()
        return self.drs.spipe.get_datasets()

    @property
    def datasets(self):
        """
        :return: shortcut for SchemaPipe operations to output datasets metadata in a dataframe
        """
        return self.get_datasets().over('nid, dim4, dim3, cname, alias, ctype, api, counter, db, path').\
            to_dataframe('dim4, dim3').out()

    def get_datamodels(self):
        """
        :return: result from  `get_datamodels` method that can be chained to other operations e.g. over(), out()
        use out() at the end of the chained methods to retrieve the final result
        """
        self.unset_filter()
        return self.dms.spipe.get_datamodels()

    @property
    def datamodels(self):
        """
        :return: shortcut for SchemaPipe operations to output datamodels metadata in a dataframe
        """
        return self.get_datamodels().over('nid, dim4, dim3, cname, alias, counter').to_dataframe('dim4, dim3').out()

    def add_vertex(self, **vprops):
        """
        :param vprops: Schema vertex properties
        :return: a vertex of Schema Graph
        """
        #  node in the hypergraph is represented with a vertex of the graph
        v = self._graph.add_vertex()

        # Add vertex properties
        for key, val in vprops.items():
            self.vertex_properties[key][v] = val

        return v

    def add_edge(self, from_vertex, to_vertex, **eprops):
        """
        :param from_vertex: tail vertex
        :param to_vertex: head vertex
        :param eprops: Schema edge properties
        :return: an edge of Schema Graph
        """
        # a hyperlink in the hypergraph is represented with an edge of the graph
        e = self._graph.add_edge(from_vertex, to_vertex)

        # Add edge properties
        if eprops:
            for key, val in eprops.items():
                self.edge_properties[key][e] = val

        return e

    def add_edges(self, elist):
        """
        Notice: it is not used in this module....

        :param elist: edge list
        :return:
        """
        return self._graph.add_edge_list(edge_list=elist, hashed=False, string_vals=False, eprops=None)

    def add_link(self, from_node, to_node, **eprops):
        """
        :param from_node: tail node is a  SchemaNode object or node ID
        :param to_node: head node is a SchemaNode object or node ID
        :param eprops: edge properties

        If there isn't a link from node, to node it will try to create a new one,
        otherwise it will  return an existing SchemaLink instance

        :return: SchemaLink object, i.e. an edge of the schema graph
        """
        schema_link = SchemaLink(schema=self, from_node=from_node, to_node=to_node, **eprops)
        # Increment the counter of the Schema Link Subsystem (SLS) node
        self.counter[self.sls.vertex] += 1
        return schema_link

    def _add_root(self, **nprops):
        """
        :param nprops: schema node (vertex) properties
        :return: Root object
        """
        # Update node properties
        nprops.update({'counter': 0, 'dim4': 0, 'dim3': 0, 'dim2': 0, 'ntype': 'SYS'})
        return System(self, **nprops)

    def _add_systems(self):
        """
        Add HyperMorph root meta-management system and subsystems
        """
        self._add_root(ctype='SYS', cname='*** HyperMorph METADATA CATALOG SYSTEM ***',
                       alias='MMS', descr='HyperMorph Root System for all data catalog containers')

        self._add_system(ctype='DS', cname='*** HyperMorph DATA RESOURCES Subsystem ***',
                         alias='DRS', descr='Data Catalog container for data sets')

        self._add_system(ctype='DM', cname='*** HyperMorph DATA MODELS Subsystem ***',
                         alias='DMS', descr='Data Catalog container for data models')

        self._add_system(ctype='HL', cname='*** HyperMorph HYPER LINKS Subsystem ***',
                         alias='HLS', descr='Data Catalog container for hyper-links')

        self._add_system(ctype='SL', cname='*** HyperMorph SCHEMA LINKS Subsystem ***',
                         alias='SLS', descr='Data Catalog container for schema-links')

        # Add edges to connect the Root system with the four subsystems we defined above
        root = System(self, 0)
        self.add_edge(root.vertex, self.drs.vertex, etype=1, ename=1, elabel=1, ealias=1)
        self.add_edge(root.vertex, self.dms.vertex, etype=1, ename=1, elabel=1, ealias=1)
        self.add_edge(root.vertex, self.hls.vertex, etype=1, ename=1, elabel=1, ealias=1)
        self.add_edge(root.vertex, self.sls.vertex, etype=1, ename=1, elabel=1, ealias=1)
        # Increment the counter of the Schema Link Subsystem (SLS) node
        self.counter[self.sls.vertex] += 4

    def _add_system(self, **nprops):
        """
        :param nprops: schema node (vertex) properties
        :return: System object
        """
        root = System(self, 0)
        # Increment the counter of the root node
        self.counter[root.vertex] += 1

        # Set object dimensions and other properties
        dim4 = self.counter[root.vertex]
        dim3 = 0
        dim2 = 0
        cnt = 0
        node_type = 'SYS'

        # Update node properties
        nprops.update({'counter': cnt, 'dim4': dim4, 'dim3': dim3, 'dim2': dim2, 'ntype': node_type})
        return System(self, **nprops)

    def add(self, what, with_components=False, datamodel=None, **kwargs):
        """
        Wrapper method for `add` methods

        :param what: the type of node to add (datamodel, entity, entities, attribute, dataset)
        :param with_components: existing components of the dataset to add, valid parameters are
                                ['tables', 'fields'], 'tables', 'graph data models', 'schemata')

                "tables": For datasets in a DBMS add database tables,
                             For datasets from files with a tabular structure add files of a specific type in a folder
                             Files with tabular structure are flat files (CSV, TSV), Parquet files, Excel files, etc...
                             Note: These are added as new Table nodes of HyperMorph Schema with type TBL
                "fields": Either add columns of a database table or fields of a file with tabular structure
                          Note: These are added as new Field nodes of HyperMorph Schema with type FLD
                "graph data models": A dataset of graph data models, i.e. files of type .graphml or .gt in a folder
                                     Each files in the set serializes, represents, HyperMorph DataModel
                "schemata": A dataset of HyperMorph schemata, i.e. files of type .graphml or .gt in a folder
                            Each file in the set serializes, represents, HyperMorph Schema

        :param datamodel: A node of type DM to add NEW nodes of type Entity and Attribute
        :param kwargs: Other keyword arguments to pass
        :return: the object(s) that were added to HyperMorph Schema
        """
        # Add datamodel
        if what == 'datamodel':
            result = self.add_datamodel(**kwargs)
        # Add entity
        elif what == 'entity':
            result = datamodel.add_entity(**kwargs)
        # Add entities
        elif what == 'entities':
            result = datamodel.add_entities(**kwargs)
        # Add attribute
        elif what == 'attribute':
            result = datamodel.add_attribute(**kwargs)
        # Add dataset
        elif what == 'dataset':
            ds = self.add_dataset(**kwargs)

            # Add existing components of the dataset
            if with_components == ['tables', 'fields']:
                ds.add_tables()
                ds.add_fields()
            elif with_components == 'tables':
                ds.add_tables()
            elif with_components == 'graph data models':
                ds.add_graph_datamodels()
            elif with_components == 'graph schemata':
                ds.add_graph_schemata()

            result = ds
        else:
            raise InvalidAddOperation(f'Failed to execute `add` method, '
                                      f'exhaustion of cases for what={what} parameter')
        return result

    def add_dataset(self, **nprops):
        """
        :param nprops: schema node (vertex) properties
        :return: DataSet object
        """
        # Parse parameters
        if 'cname' not in nprops or 'alias' not in nprops or 'ctype' not in nprops:
            raise InvalidAddOperation(f'Failed: <cname>, <alias> and <ctype> parameters are mandatory')

        ctype = nprops['ctype']
        if ctype not in container_types:
            raise InvalidAddOperation(f'Failed to add DataSet: unknown container type {ctype}')

        drs = System(self, 1)
        try:
            # Set object dimensions and other properties
            dim4 = drs.dim4
            dim3 = (drs.counter+1) * 121
            dim2 = 0
            cnt = 0
            node_type = 'DS'

            # Update node properties
            nprops.update({'counter': cnt, 'dim4': dim4, 'dim3': dim3, 'dim2': dim2, 'ntype': node_type})

            # Create new dataset
            new_dataset = DataSet(self, **nprops)
        except Exception:
            raise InvalidAddOperation(f'Failed to create DataSet object')

        # Increment the counter of the DRS node
        self.counter[drs.vertex] += 1

        # Add a link to connect DataResourceSystem with the new dataset
        self.add_link(drs, new_dataset, etype=1, ename=2, elabel=2, ealias=2)

        return new_dataset

    def add_datamodel(self, **nprops):
        """
        :param nprops: schema node (vertex) properties
        :return: DataModel object
        """
        # Parse parameters
        if 'cname' not in nprops or 'alias' not in nprops:
            raise InvalidAddOperation(f'Failed: <cname>, <alias> parameters are mandatory')

        dms = System(self, 2)
        try:
            # Set object dimensions and other properties
            dim4 = dms.dim4
            dim3 = (dms.counter+1) * 100
            dim2 = 0
            node_type = 'DM'

            # Update node properties
            nprops.update({'counter': 0, 'dim4': dim4, 'dim3': dim3, 'dim2': dim2, 'ntype': node_type})

            # Create new DataModel
            new_datamodel = DataModel(self, **nprops)
        except Exception:
            raise InvalidAddOperation(f'Failed to create DataModel object')

        # Increment the counter of the DMS node
        self.counter[self.dms.vertex] += 1

        # Add an edge to connect DataModeSystem with the new data model
        self.add_link(self.dms, new_datamodel, etype=1, ename=3, elabel=3, ealias=3)

        return new_datamodel
