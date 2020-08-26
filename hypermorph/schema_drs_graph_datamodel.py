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
import hypermorph.schema as sch
from .schema_node import SchemaNode
from .exceptions import GraphNodeError


class GraphDataModel(SchemaNode):
    # Graph Data Model is a data resource, DO NOT confuse it with HyperMorph DataModel
    # An example of GraphDataModel resource
    # is a serialized representation with a file that has <.graphml>, <.gt> format
    def __init__(self, schema, vid=None, **node_properties):
        """
        :param schema: object of type Schema
        :param node_properties: schema node (vertex) properties
        """
        # Either create (vid=None) or initialize (vid is not None) an existing SchemaNode instance
        super().__init__(schema, vid, **node_properties)

        # Existing GraphDataModel node case
        if vid is not None:
            # Check node type
            if self.ntype != 'GDM':
                raise GraphNodeError(f'Failed to initialize Graph DataModel: '
                                     f'incorrect node type {self.ntype} of vertex id = {vid}')

    def __repr__(self):
        return f'GDM[{self.nid}]'

    def __str__(self):
        return f'GDM[{self.nid}]:{self.key}'

    @property
    def parent(self):
        return self.in_nodes[0]

    def load_into_schema(self):
        """
        Load GraphDataModel data resource into TRIADB Schema in memory

        Notice:
        Do not confuse adding a set of GraphDataModels, i.e. a set of data resources with
        loading any of these graph data models into TRIADB Schema in memory.

        The last one is a different operation, it creates new TRIADB data models into Schema
        i.e. loads metadata information about the DataModel, its Entities and Attributes into TRIADB Schema

        :return: DataModel object
        """
        # Create a new Schema object to load metadata info of the data model, i.e. DataModel, Entities, Attributes
        model_schema = sch.Schema(load=True,
                                  net_name=self.cname, net_format=self.ctype.lower(), net_path=self.parent.path)

        # Fetch DataModel from model_schema
        dm = model_schema.dms.out_nodes[0]

        # Add new DataModel to TRIADB metadata schema
        new_dm = self.schema.add_datamodel(cname=dm.cname, alias=dm.alias, descr=dm.descr)

        # Add model_schema Entities to the new DataModel
        ent_meta = [dict(zip(['cname', 'alias', 'descr'],
                             [ent.cname, ent.alias, ent.descr])) for ent in dm.get_entities().to_nodes().out()]
        new_dm.add_entities(metadata=ent_meta)

        # Add model_schema Attributes to the new DataModel
        for attr in dm.get_attributes().to_nodes().out():
            attr_meta = dict(zip(['cname', 'alias', 'descr', 'extra'],
                                 [attr.cname, attr.alias, attr.descr, attr.extra]))
            # If junction attribute
            if len(attr.get_entities().to_nodes().out()) > 1:
                aliases = [ent.alias for ent in attr.get_entities().to_nodes().out()]
            else:
                aliases = attr.get_entities().to_nodes().out()[0].alias

            new_dm.add_attribute(entalias=aliases, **attr_meta)

        return new_dm
