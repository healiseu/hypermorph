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
from .schema_node import SchemaNode
from .exceptions import GraphNodeError


class GraphSchema(SchemaNode):
    """
    GraphSchema is a data resource, a child of DataSet like a Table, DO NOT confuse it with HyperMorph Schema
    An instance of GraphSchema resource is a serialized representation with a file that has <.graphml>, <.gt> format
    """
    def __init__(self, schema, vid=None, **node_properties):
        """
        :param schema: object of type Schema
        :param node_properties: schema node (vertex) properties
        """
        # Either create (vid=None) or initialize (vid is not None) an existing SchemaNode instance
        super().__init__(schema, vid, **node_properties)

        # Existing GraphSchema node case
        if vid is not None:
            # Check node type
            if self.ntype != 'GSH':
                raise GraphNodeError(f'Failed to initialize Graph Schema: '
                                      f'incorrect node type {self.ntype} of vertex id = {vid}')

    def __repr__(self):
        return f'GSH[{self.nid}]'

    def __str__(self):
        return f'GSH[{self.nid}]:{self.key}'

    @property
    def parent(self):
        return self.in_nodes[0]
