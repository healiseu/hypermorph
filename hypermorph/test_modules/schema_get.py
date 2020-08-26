"""
HyperMorph Modules Testing:
        loading HyperMorph Schema from a file with `graphml` or `gt` graph format

        subclasses of SchemaNode (OOP approach, working with objects and dot notation)
        CQL operations and get_* methods

(C) August 2020 By Athanassios I. Hatzis
"""
from hypermorph import Schema

from hypermorph.schema_node import SchemaNode
from hypermorph.schema_link import SchemaLink

from hypermorph.schema_drs_dataset import DataSet
from hypermorph.schema_drs_table import Table
from hypermorph.schema_drs_field import Field

from hypermorph.schema_dms_datamodel import DataModel
from hypermorph.schema_dms_entity import Entity
from hypermorph.schema_dms_attribute import Attribute

# Load Schema
s = Schema(debug=1, load=True, net_name='my_schema_binary', net_format='gt', net_path='/data/test/Schemata')

# =========================================================================================================
# OOP approach - Working directly with objects and dot notation
# =========================================================================================================
#
# ---------------------
# Schema constructs
# ---------------------
print(s)
SchemaNode(s, 0)
SchemaNode(s, 1)
SchemaNode(s, 2)
SchemaNode(s, 3)
SchemaNode(s, 4)
SchemaLink(s, DataSet(s, 5), Table(s, 6))
SchemaLink(s, 5, 6)

# System objects
s.systems
s.root   # System(s, 0)
s.drs    # System(s, 1) ---> data-resources subsystem
s.dms    # System(s, 2) ---> data-models subsystem
s.hls    # System(s, 3) ---> hyper-links subsystem
s.sls    # System(s, 4) ---> schema-links subsystem

# ----------------------------------
# Data Resource Subsystem constructs
# ----------------------------------
s.datasets
#
# Schema node metadata
DataSet(s, 114).all
DataSet(s, 114).descriptive_metadata
DataSet(s, 114).connection_metadata
DataSet(s, 114).system_metadata

Table(s, 115).all
Table(s, 115).system_metadata

Field(s, 11).all
Field(s, 11).metadata
Field(s, 11).parent
Field(s, 11).parent.parent
Field(s, 11).parent.parent.parent
Field(s, 11).parent.parent.parent.parent
Field(s, 11).parent.parent.parent.parent.parent

# ---------------------------------
# Data Model Subsystem constructs
# ---------------------------------
s.datamodels
#
# Schema node metadata
DataModel(s, 442).all
DataModel(s, 442).descriptive_metadata
DataModel(s, 442).system_metadata
DataModel(s, 442).parent
DataModel(s, 442).entities
DataModel(s, 442).attributes

# Accessing underlying data structures
DataModel(s, 442).out_nids
DataModel(s, 442).out_vertices
DataModel(s, 442).out_nodes
DataModel(s, 442).out_links
for link in DataModel(s, 442).all_links:
    print(link)

Entity(s, 443).all
Entity(s, 443).parent
Entity(s, 443).datamodel
Entity(s, 443).attributes

Attribute(s, 459).all
Attribute(s, 459).parent
Attribute(s, 459).datamodel
Attribute(s, 459).entities
Attribute(s, 460).entities

# Plot Objects as hypergraphs using igraph library
Table(s, 30).to_hypergraph().out(edge_label='hasField')
Entity(s, 443).to_hypergraph().out(edge_label='hasAttribute')


# =========================================================================================================
# Pipeline approach - Working with chainable functional methods (operations)
# =========================================================================================================
#
# Get any node object with a key or with node id
s.at(dim4=1, dim3=847, dim2=0)
s.get(nid=306)

s.at(dim4=2, dim3=100, dim2=10)
s.get(nid=452)

# All nodes
s.all_nodes
# all nodes of the graph in pandas dataframe
s.get_all_nodes().over('nid, dim4, dim3, dim2, cname, alias').to_dataframe('dim4, dim3, dim2').out()
# all nodes of the graph in numpy array
s.get_all_nodes().out()
s.get_all_nodes().filter_view(attribute='alias', value='SPC').to_dataframe().out()

# Overview of Systems, DataModels, DataSets
s.overview
s.get_overview().to_dataframe().out()
s.get_overview().to_dataframe('dim4, dim3, dim2').out()

# DataModels or DataSets that have alias='SPC'
s.get_overview().filter_view(attribute='alias', value='SPC').to_dataframe().out()

# Systems
s.systems
s.get_systems().to_dataframe().out()
s.get_systems().to_nodes().out()

# DataModels
s.datamodels
s.get_datamodels().to_dataframe().out()

# Get a specific DataModel with alias='SPC'
s.get_datamodels().filter_view().filter_view(attribute='alias', value='SPC', reset=False).to_dataframe().out()

# All components of a DataModel with node ID or dimensions
s.at(2, 100, 0).components
s.get(442).components
s.get(442).get_components().over(select='nid, alias, ntype').to_tuples().out()

# Entities of a DataModel
DataModel(s, 442).entities
DataModel(s, 442).get_entities().over('nid, dim3, dim2, cname, alias, descr').to_dataframe('dim3, dim2').out()

# Attributes of a DataModel
DataModel(s, 442).attributes
DataModel(s, 442).get_attributes().over('nid, dim3, dim2, cname, alias, descr, vtype, junction, parent, entities').\
    to_dataframe('dim3, dim2').out()

# Get only the junction attributes
DataModel(s, 442).get_attributes(junction=True).over('nid, dim3, dim2, cname, alias, vtype, junction, entities').\
    to_dataframe('dim3, dim2').out()
# Get all the attributes except those that are junction
DataModel(s, 442).get_attributes(junction=False).over('nid, dim3, dim2, cname, alias, vtype, junction, entities').\
    to_dataframe('dim3, dim2').out()

# Attributes of an Entity
Entity(s, 445).attributes
Entity(s, 445).get_attributes().over('nid, parent, entities, cname, alias, descr, junction').to_dataframe().out()
Entity(s, 445).get_attributes().over('nid, cname, alias, descr, junction').to_tuples().out()
Entity(s, 445).get_attributes(junction=True).to_nodes().out()
Entity(s, 445).get_attributes(junction=False).to_nodes().out()

# Entities of a junction Attribute
Attribute(s, 459).entities
Attribute(s, 459).get_entities().over('nid, parent, cname, alias, descr, ntype').to_dataframe().out()

# Entities of a non-junction Attribute
Attribute(s, 455).entities
Attribute(s, 455).get_entities().over('nid, parent, cname, alias, descr, ntype').to_dataframe().out()

# Datasets
s.datasets
s.get_datasets().over('dim4, dim3, dim2, cname, alias, ctype').to_dataframe('dim4, dim3, dim2').out()
s.get_datasets().over('nid, cname, alias, descr, ctype, path, db').to_dataframe('nid').out()
s.get_datasets().over('nid, dim3, alias, ctype').to_dict_records().out()
s.get_datasets().over('nid, dim3, alias, ctype').to_tuples().out()
s.get_datasets().over('nid, dim3, alias, ctype').to_tuples(lazy=True).out()
s.get_datasets().to_nodes().out()
s.get_datasets().to_nids().out()              # numpy array of nodes
s.get_datasets().out()                        # numpy array of nodes
s.get_datasets().to_nids(array=False).out()   # python list of nodes
s.get_datasets().to_keys().out()
s.get_datasets().to_vertices().out()
s.get_datasets().to_dict(key_column='nid', value_columns='cname, alias').out()

# Filter nodes that are DataSets, two ways to do this with filter_view() method
s.get_datasets().filter_view().to_dataframe().out()
# or
nids = s.get_datasets().to_nids().out()
s.get_all_nodes().filter_view(nids).to_dataframe().out()

# Composite filters, i.e. graph view of graph view (reset=False)
s.get_datasets().filter_view().filter_view(attribute='alias', value='SPC', reset=False).to_dataframe().out()

# Get a specific DataSet object with alias
s.get_all_nodes().filter_view(attribute='alias', value='MYGDMS').to_dataframe().out()

# # All components of a DataSet with node ID or dimensions
s.at(1, 121, 0).components
s.get(5).components
s.get(5).get_components().over('nid, parent, dim3, dim2, cname, alias, ntype, counter').to_dataframe('dim3, dim2').out()
s.get(413).get_components().over('nid, cname, alias, ntype, ctype, path').to_dataframe('nid').out()

# ================================================================================================================
# Read container metadata,
# i.e. access information stored in the data resource, or metadata from the database dictionary
#
# Tables Metadata for SQLite database container
DataSet(s, 26).container_metadata(fields='name', trace=3)
DataSet(s, 26).container_metadata(fields='name', out='tuples')
# Columns Metadata for SQLite table container
Table(s, 32).container_metadata(fields='name, type', out='tuples')

# Tables Metadata for MySQL database container
DataSet(s, 135).container_metadata()
DataSet(s, 135).container_metadata(fields='TABLE_NAME, TABLE_ROWS, AVG_ROW_LENGTH, TABLE_COLLATION', trace=3)
# Columns Metadata for MySQL table container
Table(s, 141).container_metadata()
Table(s, 141).container_metadata(fields='COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT')

# ==========================================================================================================

# Graph Schemata of a DataSet
DataSet(s, 437).graph_schemata
DataSet(s, 437).get_graph_schemata().to_dataframe('nid').out()

# Graph DataModels of a DataSet
DataSet(s, 440).graph_datamodels
DataSet(s, 440).get_graph_datamodels().to_dataframe('nid').out()

# Tables of a DataSet
DataSet(s, 135).tables
DataSet(s, 135).out_nodes
DataSet(s, 135).get_tables().over('dim3, dim2, cname, ntype, ctype, counter').to_dataframe('dim3, dim2').out()
DataSet(s, 135).get_tables().to_keys().out()
DataSet(s, 5).get_tables().to_tuples().out()

# Fields of a DataSet
DataSet(s, 5).fields
DataSet(s, 5).get_fields().over('nid, dim3, dim2, parent, cname, ntype, ctype, vtype').to_dataframe('dim3, dim2').out()
DataSet(s, 5).get_fields().to_nodes().out()


# Fields of a Table
Table(s, 6).fields
Table(s, 6).out_nodes
Table(s, 6).get_fields().over('nid, dim3, dim2, parent, cname, ntype, ctype, vtype').to_dataframe('dim3, dim2').out()
Table(s, 414).get_fields().over('nid, dim3, dim2, parent, cname, ntype, ctype, vtype').to_dataframe('dim3, dim2').out()
Table(s, 414).get_fields().over('nid, cname, vtype').\
    take('npi, pacID, profID, last, first, gender, graduated, city, state').to_dataframe(index='nid').out()

# Fields of a DataSet with value attributes
DataSet(s, 5).get_fields().over('cname, extra').to_dataframe().out()
DataSet(s, 5).get_fields().over('nid, cname, vtype, default, collation, enum, missing, unique').to_dataframe().out()
