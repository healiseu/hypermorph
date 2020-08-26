from .data_graph_node import GDataNode
from .exceptions import GraphNodeError


class HyperBond(GDataNode):
    def __init__(self, gdata, vid=None, **node_properties):
        """
        :param gdata: object of type Schema
        :param node_properties: GData node (vertex) properties
        """
        # Either create (vid=None) or initialize (vid is not None) an existing SchemaNode instance
        super().__init__(gdata, vid, **node_properties)

        # Existing HyperBond node case
        if vid is not None:
            # Check node type
            if self.ntype != 1:
                raise GraphNodeError(f'Failed to initialize HyperBond: '
                                     f'incorrect node type {self.ntype} of vertex id = {vid}')

    def __repr__(self):
        return f'HB[{self.nid}]'

    def __str__(self):
        return f'HBond[{self.nid}]:{self.key}'

