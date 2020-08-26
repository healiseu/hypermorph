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
from .schema_node import SchemaNode, field_meta
from .exceptions import GraphNodeError


class Field(SchemaNode):
    """
    Notice: all get_* methods return SchemaPipe, DataPipe objects
            so that they can be chained to other methods of those classes.
            That way we can convert, transform easily anything to many forms keys, dataframe, SchemaNode objects...
    """
    def __init__(self, schema, vid=None, **node_properties):
        """
        :param schema: object of type Schema
        :param node_properties: schema node (vertex) properties
        """
        # Either create (vid=None) or initialize (vid is not None) an existing SchemaNode instance
        super().__init__(schema, vid, **node_properties)

        # self.field = None

        # Existing Field node case
        if vid is not None:
            # Check node type
            if self.ntype != 'FLD':
                raise GraphNodeError(f'Failed to initialize Field: '
                                      f'incorrect node type {self.ntype} of vertex id = {vid}')
            else:
                pass
                # self.field = pa.field(self.cname, getattr(pa, self.vtype)())

    def __repr__(self):
        return f'Field[{self.nid}]'

    def __str__(self):
        return f'Field[{self.nid}]:{self.key}'

    @property
    def parent(self):
        return self.in_nodes[0]

    @property
    def attributes(self):
        # Return attributes where the field is mapped onto
        return self.out_nodes

    @property
    def metadata(self):
        return {col: self.get_value(col) for col in field_meta}
