"""
HyperMorph Modules Testing:
    loading HyperMorph Schema from a file with `graphml` or `gt` graph format
    Testing CQL operations and `get()` method with OOP and functional programming paradigms

(C) August 2020 By Athanassios I. Hatzis
"""
from hypermorph import MIS

# Load HyperMorph Schema, create management information system object
mis = MIS(debug=1, load=True, net_name='my_schema_binary', net_format='gt', net_path='/data/test/Schemata')
# Shortcut for schema
s = mis.mms

# =========================================================================================================
# HyperMorph OOP approach - Working directly with objects and dot notation
# =========================================================================================================
#
# mis upper level nodes
mis.root
mis.drs
mis.dms
mis.hls
mis.sls

# Get node
mis.at(1, 847, 0)
mis.get(306)

# Get node metadata
mis.get(114).all
mis.get(114).descriptive_metadata
mis.get(114).connection_metadata
mis.get(114).system_metadata
mis.get(114).container_metadata()
mis.get(11).metadata

#
# data resource container metadata
#
mis.get(135).container_metadata(fields='TABLE_NAME, TABLE_ROWS, AVG_ROW_LENGTH, TABLE_COLLATION')
mis.get(141).container_metadata(fields='COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT', trace=4)

# mis upper level methods
mis.overview
mis.all_nodes
mis.systems

# Same methods above but `get_overview()`, `get_all_nodes()`, `get_systems()` methods have a filter_view() applied
mis.get_overview().to_dataframe().out()
mis.get_all_nodes().to_dataframe().out()
mis.get_systems().to_dataframe().out()

# Datamodels
mis.datamodels
mis.get_datamodels().to_dataframe().out()

# Understand the difference between `filter` and `filter_view` methods
mis.get_datamodels().filter_view(attribute='alias', value='SPC', reset=False).to_dataframe().out()

mis.get(442).components
mis.get(442).get_components().over(select='nid, alias, ntype').to_tuples().out()
mis.get(442).entities
mis.get(442).get_entities().over('nid, dim3, dim2, cname, alias, descr').to_dataframe('dim3, dim2').out()
mis.get(442).attributes
mis.get(442).get_attributes().over('nid, dim3, dim2, cname, alias, descr, vtype, junction, parent, entities').\
    to_dataframe('dim3, dim2').out()
mis.get(442).get_attributes().over('cname').to_tuples().out()
mis.get(442).get_attributes().to_dict(key_column='nid', value_columns='cname').out()

#
# Conversions
#
mis.get(442).to_hypergraph().plot()
mis.get(442).to_hypergraph().plot(edge_label='hasAttribute')
mis.get(442).to_hypergraph().out(edge_label='hasAttribute')

# Datasets
mis.datasets
mis.get_datasets().to_dataframe().out()

# Understand the difference between `filter` and `filter_view` methods
mis.get_datasets().filter_view(attribute='alias', value='SPC', reset=False).to_dataframe().out()

mis.get(5).components
mis.get(5).get_components().over('nid, parent, dim3, dim2, cname, alias, ntype, counter').\
    to_dataframe('dim3, dim2').out()

# Understand the difference between over() and take() methods
mis.get(414).get_fields().over('nid, dim3, dim2, cname').to_dataframe('dim3, dim2').out()
mis.get(414).get_fields().over('nid, dim3, dim2, cname').\
    take(select='npi, pacID, profID, city, state').to_dataframe('dim3, dim2').out()

mis.get(437).graph_schemata
mis.get(437).get_graph_schemata().to_dataframe('nid').out()

mis.get(440).graph_datamodels
mis.get(440).get_graph_datamodels().to_dataframe('nid').out()

mis.get(135).tables
mis.get(135).get_tables().to_keys().out()
mis.get(5).fields
mis.get(5).get_fields().to_nodes().out()
mis.get(6).get_fields().over('nid, dim3, dim2, parent, cname, ntype, ctype, vtype').to_dataframe('dim3, dim2').out()

# =========================================================================================================
# HyperMorph Functional approach
# =========================================================================================================
#
# node metadata
#
mis.get(114, 'all_metadata')
mis.get(114, 'descriptive_metadata')
mis.get(114, 'connection_metadata')
mis.get(114, 'system_metadata')
mis.get(11, 'field_metadata')

#
# data resource container metadata
#
mis.get(135, 'container_metadata', select='TABLE_NAME, TABLE_ROWS, AVG_ROW_LENGTH, TABLE_COLLATION')
mis.get(141, 'container_metadata', select='COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT', out='tuples')

#
# Datamodel components
#
mis.get(442, 'components')
mis.get(442, 'components', select='nid, alias, ntype', out='tuples')
mis.get(442, 'components', filter_attribute='cname', filter_value='sid')
mis.get(442, 'entities')
mis.get(442, 'entities', out='dict', key_column='nid', value_columns='cname, alias')
mis.get(442, 'entities', 'nid, dim3, dim2, cname, alias, descr', 'dim3, dim2')
mis.get(442, 'attributes')
mis.get(442, 'attributes', 'nid, dim3, dim2, cname, alias, descr, vtype, junction, parent, entities', 'dim3, dim2')
mis.get(442, 'attributes', junction=True)

#
# DataSet components
#
mis.get(5, 'components')
mis.get(5, 'components', 'nid, parent, dim3, dim2, cname, alias, ntype, counter', 'dim3, dim2')
mis.get(437, 'graph_schemata')
mis.get(437, 'graph_schemata', index='nid')
mis.get(440, 'graph_datamodels')

mis.get(135, 'tables')
mis.get(135, 'tables', out='keys')
mis.get(5, 'fields')
mis.get(5, 'fields', out='nodes')
mis.get(6, 'fields', 'nid, dim3, dim2, parent, cname, ntype, ctype, vtype', 'dim3, dim2')
