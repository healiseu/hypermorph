"""
HyperMorph Modules Testing:
    Create a data graph by filtering ASET

(C) August 2020 By Athanassios I. Hatzis
"""
from hypermorph import MIS
from hypermorph.data_graph import GData
from graph_tool.draw import graph_draw, sfdp_layout, fruchterman_reingold_layout, \
                            arf_layout, planar_layout, radial_tree_layout

mis = MIS(debug=1, load=True, net_name='my_schema_binary', net_format='gt', net_path='/data/test/Schemata')
mis.at(2, 200, 0).entities
acat = mis.add('aset', entity=mis.at(2, 200, 1))

column_names = acat.entity.get_attributes().over('cname').to_tuples().out()
acat.reset()
acat.q.where('quantity>=200').And('price<20').filter().count().out()
acat.print_rows(select=column_names, index='catsid, catpid')

acat.update_hacols_filtered_state()
values = acat.q.over('catsid, catpid, catqnt, catchk').to_string_array(unique=True).out()
hlinks = acat.q.over('catsid, catpid, catqnt, catchk').to_hyperlinks().out()

# rebuild Data Graph
gd = GData(rebuild=True, net_name='SPC_data_graph_binary', net_format='gt', net_path='/data/test/DataGraphs')
gd.add_hyperlinks(hlinks=hlinks)
gd.add_values(string_values=values)

# Filter graph with all HyperBonds (gd.dim2.a > 10000)
# and those HyperAtoms that are in junction HACOLs, i.e. Attributes with dim2 in [2, 3]
# gd.unset_filter()
# vmask = np.logical_or(np.in1d(gd.dim2.a, [2, 3]), gd.dim2.a > 10000)
# gd.set_filter(vmask)

pos = fruchterman_reingold_layout(gd.graph)
graph_draw(gd.graph, pos, vertex_text=gd.value, vertex_font_size=12,
           vertex_shape=gd.ntype, vertex_fill_color=gd.dim2,
           vertex_pen_width=3, edge_pen_width=3, output_size=(1500, 1100))

gd.save_graph()
