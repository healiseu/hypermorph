
from .schema_node import SchemaNode
from .exceptions import GraphNodeError


class Attribute(SchemaNode):
    """
    Notice all get_* methods return node ids so that they can be converted easily to many forms
    keys, dataframe, SchemaNode objects, etc...
    """
    def __init__(self, schema, vid=None, **node_properties):
        """
        :param schema: object of type Schema
        :param node_properties: schema node (vertex) properties
        """
        # Either create (vid=None) or initialize (vid is not None) an existing SchemaNode instance
        super().__init__(schema, vid, **node_properties)

        # Existing Attribute node case
        if vid is not None:
            # Check node type
            if self.ntype != 'ATTR':
                raise GraphNodeError(f'Failed to initialize Attribute: '
                                      f'incorrect node type {self.ntype} of vertex id = {vid}')

    def __repr__(self):
        return f'Attribute[{self.nid}]'

    def __str__(self):
        return f'Attribute[{self.nid}]:{self.key}'

    @property
    def parent(self):
        return [obj for obj in self.in_nodes if obj.ntype == 'DM'][0]

    @property
    def fields(self):
        return [obj for obj in self.in_nodes if obj.ntype == 'FLD']

    @property
    def datamodel(self):
        return self.parent

    @property
    def entities(self):
        """
        Notice: This has a different output < out('node') >, i.e. not metadata in dataframe, because
        we use this property in projection. For example in DataSet.get_attributes.....
        :return: shortcut for SchemaPipe operations to output Entity nodes
        """
        return self.get_entities().to_nodes().out()

    def get_entities(self):
        """
        :return: result from `get_entities` method that can be chained to other operations e.g. over(), out()
        use out() at the end of the chained methods to retrieve the final result
        """
        return self.spipe.get_entities()
