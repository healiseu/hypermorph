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
import numpy as np
import hypermorph.schema_node as sn
from .exceptions import GraphLinkError

ep_names = ['etype', 'ename', 'elabel', 'ealias']


class SchemaLink(object):
    """
        Each instance of SchemaLink links a tail node with a head node, examples:
        (DataModel ---> Entity), (Entity ---> Attribute), (Field ---> Attribute),
        (Table ---> Field), (DataSet ---> Table)

        Each SchemaLink has two connectors (bidirectional edges):
        An outgoing edge from the tail
        An incoming edge to the head

        In the case of a HyperEdge (HE) node there are <Many> Outgoing Edges that start < From One > HE
        In the case of a HyperNode (HN) node there are <Many> Incoming Edges that end < To One > HN

        SchemaLink type represents a DIRECTED MANY TO MANY RELATIONSHIP

        Important Notice:
        Do not confuse the DIRECTION OF RELATIONSHIP with the
        DIRECTION OF TRAVERSING THE BIDIRECTIONAL EDGES of the SchemaLink

        Many-to-Many Relationship is defined as a (Many-to-One) and (One-to-Many)

        MANY side (tail node)    ---ONE side (outgoing edge)--- ---ONE side (incoming edge)---  MANY side (head node)

                                 (fromID)
                                  An outgoing edge
         FROM Node             ========================== SchemaLink ==========================>      TO Node
                                                                            (toID)
                                                                            An incoming edge
    """
    def __init__(self, schema, from_node, to_node, **eprops):
        """
        :param schema: object of type Schema (Graph)
        :param from_node: tail node is a SchemaNode object or node ID
        :param to_node: head node is a SchemaNode object or node ID
        :param eprops: edge properties
        """
        self._schema = schema
        self._edge = None

        # Initialize instance variables, i.e. properties of the SchemaLink instance
        self.etype, self.ename, self.elabel, self.ealias = [None]*4

        if all(isinstance(n, int) for n in (from_node, to_node)) or \
                all(isinstance(n, np.int64) for n in (from_node, to_node)):
            self._tail = sn.SchemaNode(self._schema, from_node)
            self._head = sn.SchemaNode(self._schema, to_node)
        else:
            self._tail = from_node
            self._head = to_node

        # Existing SchemaLink case
        # First try to initialize an existing edge
        try:
            # Fetch graph edge
            self._edge = schema.graph.edge(self._tail.nid, self._head.nid)
            # If the edge exists
            if self._edge:
                # Update properties of the SchemaLink instance
                self._update_schema_link_properties()
        except ValueError:
            raise GraphLinkError(f'Failed to initialize SchemaLink from {self._tail} to {self._head}')

        # New SchemaLink case
        # if the edge does not exist, then try to create it
        if not self._edge:
            try:
                # Check mandatory SchemaLink properties
                for prop_name in ep_names:
                    if prop_name not in eprops:
                        raise GraphLinkError(f'Failed to create SchemaLink with {eprops}, missing '
                                              f'a mandatory edge property (etype, ename, elabel, ealias)')
                # Create a new edge (link) in the graph with edge properties
                self._edge = schema.add_edge(self._tail.vertex, self._head.vertex, **eprops)
                # Update properties of the SchemaLink instance
                self._update_schema_link_properties()
            except ValueError:
                raise GraphLinkError(f'Failed to create SchemaLink from {self._tail} to {self._head}')

    def __repr__(self):
        return f'SL< {self._tail.ntype}[{self._tail.nid}] ---> {self._head.ntype}[{self._head.nid}] >'

    def __str__(self):
        return f'SL< {self._tail.ntype}[{self._tail.nid}]{self._tail.key} --{self.ealias}-> ' \
               f'{self._head.ntype}[{self._head.nid}]{self._head.key} >'

    def _update_schema_link_properties(self):
        """
            Update: edge properties dynamically as properties of the link
            `setattr` here will enable public access of these properties
            this way we avoid to write @property decorators for each one of them
        :return:
        """
        # Update edge properties
        [setattr(self, ep_name, self.get_value(ep_name)) for ep_name in ep_names]

    def get_value(self, prop_name):
        """
        :param prop_name: Edge property name  (ep_names)
        :return: the value of property for the specific link
        """
        if prop_name in ep_names:
            result = self._schema.get_ep_value(prop_name, self._edge)
        else:
            raise GraphLinkError(f'Failed to get link value for property {prop_name}')

        return result

    def get_edge_property(self, property_name):
        """
        this is used to access values that are returned from @properties of SchemaLink
        :param property_name: function name of the @property decorator
        :return:
        """
        return getattr(self, property_name)

    @property
    def schema(self):
        return self._schema

    @property
    def edge(self):
        return self._edge

    @property
    def all(self):
        return {col: self.get_value(col) for col in ep_names}
