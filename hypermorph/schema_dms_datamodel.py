
from .schema_node import SchemaNode
from .schema_dms_entity import Entity
from .schema_dms_attribute import Attribute
from .exceptions import GraphNodeError


class DataModel(SchemaNode):
    """
    Notice: all get_* methods return SchemaPipe, DataPipe objects
            so that they can be chained to other methods of those classes.
            That way we can convert, transform easily anything to many forms keys, dataframe, SchemaNode objects...
    ToDo: A method of DataModel to save it separately from Schema,
          e.g. write it on disk with a serialized format (graphml) or in a database....
          In the current version DataModel can be created with commands and saved in a `.graphml`, `.gt` file
          or it can be saved together with the Schema in a `.graphml`, `.gt` file
    """
    def __init__(self, schema, vid=None, **node_properties):
        """
        :param schema: object of type Schema
        :param node_properties: schema node (vertex) properties
        """
        # Either create (vid=None) or initialize (vid is not None) an existing SchemaNode instance
        super().__init__(schema, vid, **node_properties)

        # Existing DataModel node case
        if vid is not None:
            # Check node type
            if self.ntype != 'DM':
                raise GraphNodeError(f'Failed to initialize DataModel: '
                                     f'incorrect node type {self.ntype} of vertex id = {vid}')

    def __repr__(self):
        return f'DataModel[{self.nid}]'

    def __str__(self):
        return f'DataModel[{self.nid}]:{self.key}'

    @property
    def parent(self):
        return self.in_nodes[0]

    @property
    def components(self):
        """
        :return: shortcut for SchemaPipe operations to output components metadata of the datamodel in a dataframe
        """
        return self.get_components().over('nid, dim4, dim3, dim2, cname, alias, ntype, vtype, counter').\
            to_dataframe('dim4, dim3, dim2').out()

    def get_components(self):
        """
        :return: result from  `get_components` method that can be chained to other operations e.g. over(), out()
        use out() at the end of the chained methods to retrieve the final result
        """
        return self.spipe.get_components()

    @property
    def attributes(self):
        """
        :return: shortcut for SchemaPipe operations to output metadata in a dataframe
        """
        return self.get_attributes(junction=None).over('nid, dim3, dim2, cname, alias, vtype, fields').\
            to_dataframe('dim3, dim2').out()

    def get_attributes(self, junction=None):
        """
        :return: result from  `get_attributes` method that can be chained to other operations e.g. over(), out()
        use out() at the end of the chained methods to retrieve the final result
        """
        return self.spipe.get_attributes(junction=junction)

    @property
    def entities(self):
        """
        :return: shortcut for SchemaPipe operations to output metadata in a dataframe
        """
        return self.get_entities().over('nid, dim3, dim2, cname, ntype, counter').to_dataframe('dim3, dim2').out()

    def get_entities(self):
        """
        :return: result from  `get_entities` method that can be chained to other operations e.g. over(), out()
        use out() at the end of the chained methods to retrieve the final result
        """
        return self.spipe.get_entities()

    def add_entities(self, metadata):
        """
        :param metadata: list of dictionaries, dictionary keys are property names of Entity node (cname, alias, ...)
        :return: Entity objects
        """
        entities = []
        if isinstance(metadata, list):
            for d in metadata:
                if type(d) == dict:
                    entities.append(self.add_entity(**d))
                else:
                    GraphNodeError('Failed to add entities, metadata must be a list of dictionaries')
        else:
            raise GraphNodeError('Failed to add entities, metadata must be a list of dictionaries')

        return entities

    def add_entity(self, **nprops):
        """
        :param nprops: schema node (vertex) properties
        :return: single Entity object
        """
        # Parse parameters
        if 'cname' not in nprops or 'alias' not in nprops:
            raise GraphNodeError(f'Failed to add table: <cname> and <alias> parameters are mandatory')

        try:
            # Set object dimensions and other properties
            dim4 = self.dim4
            dim3 = self.dim3
            dim2 = self.get_value('counter') + 1
            cnt = 0
            node_type = 'ENT'

            # Update node properties
            nprops.update({'counter': cnt, 'dim4': dim4, 'dim3': dim3, 'dim2': dim2, 'ntype': node_type})

            # Create new Entity
            new_entity = Entity(self.schema, **nprops)
        except Exception:
            raise GraphNodeError(f'Failed to create Entity object')

        # Increment the counter of the DataSet node
        self.schema.counter[self.vertex] += 1

        # link DataModel (from node - tail) to Entity (to node - head), i.e. create edges
        self.schema.add_link(self, new_entity, etype=1, ename=7, elabel=7, ealias=7)

        return new_entity

    def add_attribute(self, entalias, **nprops):
        """
        :param entalias: Attribute is linked to Entities with the corresponding aliases
        :param nprops: schema node (vertex) properties
        :return: single Attribute object
        """
        # Parse parameters
        if 'cname' not in nprops or 'alias' not in nprops:
            raise GraphNodeError(f'Failed to add attribute: <cname> and <alias> parameters are mandatory')

        # Set the junction property
        if type(entalias) == list:
            nprops['extra']['junction'] = 1

        try:
            # Set object dimensions and other properties
            dim4 = self.dim4
            dim3 = self.dim3
            dim2 = self.get_value('counter')+1
            cnt = 0
            node_type = 'ATTR'

            # Update node properties
            nprops.update({'counter': cnt, 'dim4': dim4, 'dim3': dim3, 'dim2': dim2, 'ntype': node_type})

            # Create new attribute
            new_attribute = Attribute(self.schema, **nprops)
        except Exception:
            raise GraphNodeError(f'Failed to create Field object')

        # Increment the counter of the DataModel node
        self.schema.counter[self.vertex] += 1

        # link DataModel (from node - tail) to Attribute (to node - head), i.e. create edges
        self.schema.add_link(self, new_attribute, etype=1, ename=8, elabel=8, ealias=8)

        # link junction Attribute to Entities
        if new_attribute.junction:
            [self.schema.add_link(ent, new_attribute, etype=1, ename=9, elabel=9, ealias=9)
             for ent in self.get_entities().to_nodes().out() if ent.alias in entalias]
        else:
            [self.schema.add_link(ent, new_attribute, etype=1, ename=9, elabel=9, ealias=9)
             for ent in self.get_entities().to_nodes().out() if ent.alias == entalias]

        return new_attribute

    def to_hypergraph(self):
        return self.spipe.to_hypergraph()
