
from .schema_node import SchemaNode
from .exceptions import GraphNodeError


class Entity(SchemaNode):
    """
    Notice:
    all get_* methods return SchemaPipe, DataPipe objects so that they can be chained to other methods of those classes
    That way we can convert, transform easily anything to many forms keys, dataframe, SchemaNode objects...
    """
    def __init__(self, schema, vid=None, **node_properties):
        """
        :param schema: object of type Schema
        :param node_properties: schema node (vertex) properties
        """
        # Either create (vid=None) or initialize (vid is not None) an existing SchemaNode instance
        super().__init__(schema, vid, **node_properties)

        # Existing Entity node case
        if vid is not None:
            # Check node type
            if self.ntype != 'ENT':
                raise GraphNodeError(f'Failed to initialize Entity: '
                                      f'incorrect node type {self.ntype} of vertex id = {vid}')

    def _mapped_to_str(self):
        if self.has_mapping():
            return 'Mapped'
        else:
            return ''

    def __repr__(self):
        return f'Entity[{self.nid}] {self._mapped_to_str()}'

    def __str__(self):
        return f'Entity[{self.nid}]:{self.key} {self._mapped_to_str()}'

    @property
    def parent(self):
        return self.in_nodes[0]

    @property
    def datamodel(self):
        return self.parent

    @property
    def attributes(self):
        """
        :return: shortcut for SchemaPipe operations to output metadata in a dataframe
        """
        # junction is set to None so that all attributes are returned
        return self.get_attributes(junction=None).over('nid, dim3, dim2, cname, alias, vtype, fields').\
            to_dataframe('dim3, dim2').out()

    def get_attributes(self, junction=None):
        """
        :param junction:
            True return junction Attributes,
            False return non-junction Attributes
            None return all Attributes
        :return: return result from `get_attributes` method that can be chained to other operations e.g. over(), out()
        use out() at the end of the chained methods to retrieve the final result
        """
        return self.spipe.get_attributes(junction=junction)

    def get_fields(self, junction=None):
        """
        :param junction:
            True return fields mapped on junction Attributes,
            False return fields mapped on non-junction Attributes
            None return all fields mapped on Attributes
        :return: Fields (node ids) that are mapped onto Attributes
        Notice: In the general case, fields are mapped from more than one DataSet, Table, objects
        """
        return self.spipe.get_attributes(junction=junction).to_nodes().to_fields()

    def get_tables(self):
        """
        From the fields mapped on non-junction Attributes find its parents, i.e. tables
        ToDo: Cover the case for fields from multiple tables mapped on attributes of the same entity
        :return: Table objects
        """
        # Algorithm needs to be revised here....
        # Take the first non-junction attribute and return its parent, i.e. Table
        tables = [self.get_fields(junction=False).to_nodes().out()[0].parent]
        return tables

    def to_hypergraph(self):
        return self.spipe.to_hypergraph()

    def has_mapping(self):
        """
        :return: True if there are Field(s) of a Table mapped onto Attribute(s) of an Entity, otherwise False
        """
        if self.get_fields().out():
            return True
        else:
            return False
