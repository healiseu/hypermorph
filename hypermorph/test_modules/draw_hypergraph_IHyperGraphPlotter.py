"""
HyperMorph Modules Testing: drawing a hypergraph with IHyperGraphPlotter
(C) August 2020 By Athanassios I. Hatzis
"""
from hypermorph.draw_hypergraph import IHyperGraphPlotter

edges = [(462, 471), (462, 472), (462, 473), (462, 474), (462, 475), (462, 484)]
labels = {462: 'SUP', 471: 's_name', 472: 's_address', 473: 's_city', 474: 's_country', 475: 's_status', 484: 's_ID'}
colors = {462: 'red', 471: 'green', 472: 'green', 473: 'green', 474: 'green', 475: 'green', 484: 'green'}

gp = IHyperGraphPlotter(edges, labels, colors)

gp.plot(edge_label='hasATTR', edge_label_size=14)
