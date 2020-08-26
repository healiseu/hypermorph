"""
HyperMorph Demo:
    Add an Associative Entity Set (ASET) from an existing Entity
    The Entity has mapping, it is dictionary encoded and it is saved on disk with a feather format

    HyperAtomCollection (HACOL) operations:
        Count atoms (values) - included, unique, instances, missing, total

    Associative Entity Set (ASET) operations:

(C) August 2020 By Athanassios I. Hatzis
"""
from hypermorph import MIS
from hypermorph.data_graph import GData
from graph_tool.draw import graph_draw, sfdp_layout, fruchterman_reingold_layout, \
                            arf_layout, radial_tree_layout

# Load Schema
mis = MIS(debug=1, load=True, net_name='demo_schema', net_format='gt', net_path='data/DemoData/Schemata')
mis.at(2, 200, 0).components

#
# Add Associative Entity Set (ASET)
#
phys = mis.add('aset', entity=mis.at(2, 200, 1))
phys.attributes
phys.data
phys.hacols
phys.memory_usage(mb=True)

# HyperAtomCollection (HACOL) operations:
city = phys.city.pipe
gender = phys.gender.pipe
graduated = phys.graduated.pipe
spec0 = phys.spec0.pipe
lbn1 = phys.lbn1.pipe

cname_list = 'npi, pacID, spec0, groupTotal, last, first, gender, graduated, city, state, lbn1, ehr, pqrs'
as_names = 'id1, id2, specialty, total, last_name, first_name, sex, year, usa_city, usa_state, company, ehr, pqrs'


# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
# ***************************************************************************************
#
# Operations in unfiltered State
#
# ***************************************************************************************
# \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\

# Transformation with projection and slicing
phys.q.over(cname_list, as_names).slice(20).to_record_batch().\
    to_dataframe(index='id1, id2', order_by='usa_city, last_name, first_name').out()

# ===================================================================
# Normal Filtering mode
# ===================================================================
# 1st filter USA Physicians in Atlanta - 13199
# HyperMorph Wall time 250ms
phys.q.where('city like ATLANTA').filter().over(cname_list).to_record_batch().\
    to_dataframe(order_by='city, last, first', limit=20, index='npi, pacID').out()
# or simply count
phys.q.where('city like ATLANTA').filter().count().out()
# 2nd filter Anesthesiologists in Atlanta - 1455 hbs
phys.q.where('spec0 like ANESTH').filter().count().out()
# 3rd filter Anesthesiologists in Atlanta graduated after 2010 - 499 hbs
phys.q.where('graduated>2010').filter().count().out()
# 4th filter Male Anesthesiologists in Atlanta graduated after 2010 - 298 hbs
phys.q.where('gender like F').filter().count().out()
# 5th filter Female Anesthesiologists in Atlanta graduated after 2010 that work in GRADY MEMORIAL HOSPITAL - 46 hbs
phys.q.where('lbn1 like GRADY').filter().count().out()
# 6th filter Female Anesthesiologists in Atlanta graduated after 2010 that work in GRADY MEMORIAL HOSPITAL
# and are registered in Medicare PQRS (Physician Quality Reporting System)
phys.q.where('pqrs like Y').filter().count().out()
# Display result set
phys.print_rows(select=cname_list, order_by='last, first', index='npi, pacID')

# HACOL operations, update filtering state of HACOLs
phys.update_hacols_filtered_state()
phys.first.q.filter(phys.mask).count().out()
phys.first.q.filter(phys.mask).to_array(unique=True, order='asc').out()

#
# And again with conjuctive filtering criteria and different order
#
cond1 = 'city like ATLANTA'
cond2 = 'spec0 like ANESTH'
cond3 = 'graduated>2010'
cond4 = 'gender like F'
cond5 = 'lbn1 like GRADY'
cond6 = 'pqrs like Y'
cond7 = 'spec0 like ANESTHETIST'

phys.reset()
phys.q.where(cond2).And(cond4).And(cond3).And(cond6).And(cond1).And(cond5).filter().count().out()

# Display result set
phys.print_rows(select=cname_list, order_by='last, first', index='npi, pacID')

# ===================================================================
# Associative Filtering mode
# ===================================================================
phys.reset()
phys.select.where(cond2).filter().count().out()      # 180791 - 490ms
phys.select.where(cond4).filter().count().out()      #  76075 - 222ms
phys.select.where(cond3).filter().count().out()      #  23805 - 120ms
phys.select.where(cond6).filter().count().out()      #  14262 - 327ms
phys.select.where(cond1).filter().count().out()      #    117 - 432ms
phys.select.where(cond5).filter().count().out()      #     32 - 488ms
phys.select.where(cond7).filter().count().out()      #     10

phys.print_rows(select=cname_list, order_by='last, first', index='npi, pacID')

# States
phys.spec0.print_states()
phys.last.print_states()
phys.last.q.filter(phys.mask).count().out()
phys.last.q.to_array(unique=True, order='asc').out()

# Data Graph
phys.update_hacols_filtered_state()
values = phys.q.over('npi, last, first, groupTotal').to_string_array(unique=True).out()
hlinks = phys.q.over('npi, last, first, groupTotal').to_hyperlinks().out()

gd = GData(rebuild=True, net_name='phys_data_graph_binary', net_format='gt', net_path='/data/test/DataGraphs')
gd.add_hyperlinks(hlinks=hlinks)
gd.add_values(string_values=values)

pos = arf_layout(gd.graph)
graph_draw(gd.graph, pos, vertex_text=gd.value, vertex_font_size=12,
           vertex_shape=gd.ntype, vertex_fill_color=gd.dim2,
           vertex_pen_width=3, edge_pen_width=3, output_size=(1500, 1100))

gd.save_graph()
