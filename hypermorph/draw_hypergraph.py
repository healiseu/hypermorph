from igraph import Graph, plot


class IHyperGraphPlotter(object):
    """
    This module draws a hypergraph from edges using the igraph library
    """
    def __init__(self, edges, vertex_labels, vertex_colors):
        """
        :param edges: iterable for edges, each edge is represented with a pair of integers
        :param vertex_colors: vertex color dictionary, keys are vertex indices and values are the colors of vertices
        :param vertex_labels: vertex label dictionary, keys are vertex indices and values are the labels of vertices

        mis.get(nid=462, what='attributes', project_index='nid', project_over='alias', out='key:value')
        """
        g = Graph(edges=edges, directed=True)
        [g.vs[vid].update_attributes(label=label) for vid, label in vertex_labels.items()]
        [g.vs[vid].update_attributes(color=color) for vid, color in vertex_colors.items()]
        self._hg = g.subgraph_edges(edges=edges)

    def plot(self, **kwargs):
        """
        :param kwargs: pass parameters to igraph plot function
        :return:
        """
        # Default parameters
        visual_style = {'vertex_size': 90,
                        'vertex_shape': 'circle', 'vertex_label_size': 14,
                        'vertex_label_angle': 90, 'vertex_label_dist': 0,
                        'edge_width': 2, 'edge_arrow_size': 1.5, 'edge_arrow_width': 1, 'edge_curved': 0,
                        'edge_label': None, 'edge_label_dist': 0, 'edge_label_angle': 0, 'edge_label_size': 11,
                        'bbox': (1200, 1000), 'margin': 80, 'layout': 'kk'}

        # Override default values
        for k, v in kwargs.items():
            visual_style[k] = v

        # Plot the graph
        return plot(self._hg, **visual_style).show()
