"""
HyperMorph Modules Testing:
    Load a data graph and examine its structure

(C) August 2020 By Athanassios I. Hatzis
"""
import numpy as np
from hypermorph.data_graph import GData
from graph_tool.draw import graph_draw, sfdp_layout, fruchterman_reingold_layout, \
                            arf_layout, planar_layout, radial_tree_layout

gd = GData(load=True, net_name='SPC_data_graph_binary', net_format='gt', net_path='/data/test/DataGraphs')

# ==================================================
# Test for GData module
# ==================================================
gd.list_properties
gd.vertices
gd.vids
gd.dim2.a
gd.dim1.a
gd.ntype.a
gd.value
np.array([gd.value[v] for v in gd.vertices], dtype='str')

[gd.get(vid) for vid in gd.vids]

# HyperAtom
gd.get(1)
gd.get(1).all
gd.get(1).key
gd.at(2, 0)
gd.get(1).in_nids
gd.get(1).in_nodes
gd.get(1).in_links
gd.get(1).in_vertices

# HyperBond
gd.get(0).all
gd.get(0).key
gd.get(0).all_nids
gd.get(0).all_nodes
gd.get(0).all_links
gd.get(0).all_vertices

pos = fruchterman_reingold_layout(gd.graph)
graph_draw(gd.graph, pos, vertex_text=gd.value, vertex_font_size=12,
           vertex_shape=gd.ntype, vertex_fill_color=gd.dim2,
           vertex_pen_width=3, edge_pen_width=3, output_size=(1500, 1100))
