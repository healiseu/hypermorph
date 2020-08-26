import numpy as np
import hypermorph.data_graph_node as gdn
from .exceptions import GraphLinkError


class GDataLink(object):
    """
        Each instance of GDataLink links a tail node with a head node, i.e. HyperBond ---> HyperAtom

        Each GDataLink has two connectors (bidirectional edges):
        An outgoing edge from the tail
        An incoming edge to the head

        In the case of a HyperBond (HB) node there are <Many> Outgoing Edges that start < From One > HB
        In the case of a HyperAtom (HA) node there are <Many> Incoming Edges that end < To One > HA

        GDataLink type represents a DIRECTED MANY TO MANY RELATIONSHIP

        Important Notice:
        Do not confuse the DIRECTION OF RELATIONSHIP with the
        DIRECTION OF TRAVERSING THE BIDIRECTIONAL EDGES of the GDataLink

        Many-to-Many Relationship is defined as a (Many-to-One) and (One-to-Many)

        MANY side (tail node)    ---ONE side (outgoing edge)--- ---ONE side (incoming edge)---  MANY side (head node)

                                 (fromID)
                                  An outgoing edge
         FROM Node             ========================== GDataLink ==========================>      TO Node
                                                                            (toID)
                                                                            An incoming edge
    """
    def __init__(self, gdata, from_node, to_node):
        """
        :param gdata: object of type GData (Graph)
        :param from_node: tail node is a GDataNode object or node ID
        :param to_node: head node is a GDataNode object or node ID
        """
        self._gdata = gdata
        self._edge = None

        if all(isinstance(n, int) for n in (from_node, to_node)) or \
                all(isinstance(n, np.int64) for n in (from_node, to_node)):
            self._tail = gdn.GDataNode(self._gdata, from_node)
            self._head = gdn.GDataNode(self._gdata, to_node)
        else:
            self._tail = from_node
            self._head = to_node

        # Existing GDataLink case
        # First try to initialize an existing edge
        try:
            # Fetch graph edge
            self._edge = gdata.graph.edge(self._tail.nid, self._head.nid)
        except ValueError:
            raise GraphLinkError(f'Failed to initialize GDataLink from {self._tail} to {self._head}')

        # New GDataLink case
        # if the edge does not exist, then try to create it
        if not self._edge:
            try:
                # Create a new edge (link) in the graph with edge properties
                self._edge = gdata.add_edge(self._tail.vertex, self._head.vertex)
            except ValueError:
                raise GraphLinkError(f'Failed to create GDataLink from {self._tail} to {self._head}')

    def __repr__(self):
        return f'GDL< HB[{self._tail.nid}] ---> HA[{self._head.nid}] >'

    def __str__(self):
        return f'GDL< HB[{self._tail.nid}]{self._tail.key} ---> HA[{self._head.nid}]{self._head.key} >'

    @property
    def gdata(self):
        return self._gdata

    @property
    def edge(self):
        return self._edge
