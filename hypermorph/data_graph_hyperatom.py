"""
This file is part of HyperMorph operational API for information management and data transformations
on Associative Semiotic Hypergraph Development Framework
(C) 2015-2020 Athanassios I. Hatzis

HyperMorph is free software: you can redistribute it and/or modify it under the terms of
the GNU Affero General Public License v.3.0 as published by the Free Software Foundation.

HyperMorph is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with HyperMorph.
If not, see <https://www.gnu.org/licenses/>.
"""
from .data_graph_node import GDataNode
from .exceptions import GraphNodeError


class HyperAtom(GDataNode):
    def __init__(self, gdata, vid=None, **node_properties):
        """
        :param gdata: object of type Schema
        :param node_properties: GData node (vertex) properties
        """
        # Either create (vid=None) or initialize (vid is not None) an existing GDataNode instance
        super().__init__(gdata, vid, **node_properties)

        # Existing HyperAtom node case
        if vid is not None:
            # Check node type
            if self.ntype != 0:
                raise GraphNodeError(f'Failed to initialize HyperAtom: '
                                     f'incorrect node type {self.ntype} of vertex id = {vid}')

    def __repr__(self):
        return f'HA[{self.nid}]'

    def __str__(self):
        return f'HAtom[{self.nid}]:{self.key}'

