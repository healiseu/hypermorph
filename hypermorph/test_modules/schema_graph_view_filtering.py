"""
HyperMorph Modules Testing:
        three ways to filter GraphView with a vertex filter
        API reference: https://graph-tool.skewed.de/static/doc/graph_tool.html#graph_tool.GraphView

(C) August 2020 By Athanassios I. Hatzis
"""
from graph_tool import GraphView
from graph_tool.generation import triangulation
from graph_tool.topology import min_spanning_tree
from graph_tool.centrality import betweenness
import numpy as np
from hypermorph import Schema

s = Schema(load=True, net_name='my_schema_binary', net_format='gt', net_path='/data/test/Schemata')

# ------------------------------------
# Two ways to specify vertex filter
# ------------------------------------
# Ex1. Fetch all vertex ids from data resource system (drs) SchemaNode
vids = s.drs.out_nids
s.vmask.a[vids] = 1
s._graph_view = GraphView(s.graph, vfilt=s.vmask)
s._graph_view.get_vertices()

# Ex2. Fetch all vertex ids where SchemaNode.dim3=484 (NorthWind Traders MYSQL data set)
# a. use boolean valued PropertyMap
s._vmask = (s.dim3.a == 484)
# or
s._vmask = s.get_vp_values('dim3') == 484
GraphView(s.graph, vfilt=s.get_vp_values('dim3') == 484)

#  b. unary function that returns True if a given vertex/edge is to be selected, or False otherwise.
GraphView(s.graph, vfilt=lambda vid: s.get_vp('dim3')[vid] == 484)

# Note that, differently from the case above and below, this is an O(E) operation,
# where E is the number of edges, since the supplied function must be called E times to construct a filter property map.
# Thus, supplying a constructed filter map is always faster, but supplying a function can be more convenient.
# Using %timeit we get Ex2a: 1.69 msec, Ex2b: 6.46msec


#
# see also the following graph-tool documentation example
#
points = np.random.random_sample((500, 2))*4
g, pos = triangulation(points, type="delaunay")
tree = min_spanning_tree(g)  # EdgePropertyMap object with value type 'bool'

# Filter the Graph
g.set_edge_filter(tree)

# The original graph can be recovered by setting the edge filter to None.
g.set_edge_filter(None)

# Create a GraphView
gv = GraphView(g, efilt=tree)

#  one can supply a function as filter parameter,
#  which takes a vertex or an edge as single parameter,
#  and returns True if the vertex/edge should be kept and False otherwise.
#  For instance, if we want to keep only the most “central” edges, we can do:
bv, be = betweenness(g)
be_max = be.a.max() / 2
u1 = GraphView(g, efilt=lambda e: be[e] > be_max)

# Note that, differently from the case above and below, this is an O(E) operation,
# where E is the number of edges, since the supplied function must be called E times to construct a filter property map.
# Thus, supplying a constructed filter map is always faster, but supplying a function can be more convenient.

# compare with specifyiing a filter as a boolean-valued PropertyMap
be_mask = be.a > be_max   # boolean-valued PropertyMap
u2 = GraphView(g, efilt=be_mask)
