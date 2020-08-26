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


class System(SchemaNode):
    def __init__(self, schema, vid=None, **node_properties):
        """
        :param schema: object of type Schema
        :param node_properties: schema node (vertex) properties
        """
        # Either create (vid=None) or initialize (vid is not None) an existing SchemaNode instance
        super().__init__(schema, vid, **node_properties)

        # Existing System node case
        if vid is not None:
            # Check node type
            if self.ntype != 'SYS':
                raise GraphNodeError(f'Failed to initialize System: '
                                      f'incorrect node type {self.ntype} of vertex id = {vid}')

    def __repr__(self):
        return f'System[{self.nid}]'

    def __str__(self):
        return f'System[{self.nid}]:{self.key}'

    @property
    def parent(self):
        if self.dim4 == 0:
            # Parent of Root System is the Root System
            return System(self.schema, 0)
        else:
            return self.in_nodes[0]

    @property
    def datasets(self):
        if isinstance(self, System) and self.dim4 == self.schema.drs.dim4:
            return self.schema.datasets
        else:
            raise GraphNodeError(f'Failed to fetch datasets for object {self} with type {self.ntype}')

    @property
    def datamodels(self):
        if isinstance(self, System) and self.dim4 == self.schema.dms.dim4:
            return self.schema.datamodels
        else:
            raise GraphNodeError(f'Failed to fetch datamodels for object {self} with type {self.ntype}')

    @property
    def systems(self):
        if isinstance(self, System) and self.dim4 == 0:
            return self.schema.systems
        else:
            raise GraphNodeError(f'Failed to fetch systems for object {self} with type {self.ntype}')
