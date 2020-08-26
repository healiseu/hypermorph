"""
HyperMorph Demo:
    Schema operations

(C) August 2020 By Athanassios I. Hatzis
"""
from hypermorph import MIS

# Load schema
mis = MIS(debug=1, load=True, net_name='demo_schema', net_format='gt', net_path='data/DemoData/Schemata')

# display the schema of these objects you created
mis.overview

# or view their components on a dataframe
mis.get(136).get_components().over('nid, dim3, dim2, ntype, parent, cname, alias, vtype').to_dataframe(index='dim3, dim2').out()

# or view their components as tuples
mis.get(136).get_components().over('nid, dim3, dim2, ntype, parent, cname, alias, vtype').to_tuples().out()

# or as objects
mis.get(136).get_components().to_nodes().out()

# or in a key:value dictionary
mis.at(1, 484, 0).get_components().to_dict(key_column='nid', value_columns='dim3, dim2, ntype, parent, cname, alias, vtype').out()

# or in records (list of dictionaries)
mis.at(1, 484, 1).get_fields().over('nid, dim3, cname, ctype, vtype').to_dict_records().out()

# or even plotted on a hypergraph
mis.at(1, 484, 1).get_fields().to_hypergraph().out(edge_label='hasField')

# EVERYTHING you see on the schema is a node of a graph
mis.at(1, 484, 1)

# with SchemaLink(s) to outward nodes
mis.at(1, 484, 1).out_nodes
mis.at(1, 484, 1).out_links

# and with SchemaLink(s) to inward nodes
mis.at(1, 484, 1).in_nodes
mis.at(1, 484, 1).in_nodes

# Operations on DataModel
mis.at(2, 100, 0).get_components().over('nid, dim3, dim2, ntype, parent, cname, alias, vtype').to_dataframe(index='dim3, dim2').out()
mis.at(2, 100, 0).get_components().to_hypergraph().out(edge_label='hasAttribute')
mis.at(2, 100, 0).get_attributes(junction=True).over('nid, parent, entities, cname, alias, descr, junction').to_dataframe().out()
mis.get(261).in_nodes
mis.get(261).in_links

