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
from .utils import FileUtils
from . import flat_file_types, container_types, db_types
from .exceptions import GraphNodeError
from .schema_node import SchemaNode
from .schema_drs_field import Field


class Table(SchemaNode):
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

        # Existing Table node case
        if vid is not None:
            # Check node type
            if self.ntype != 'TBL':
                raise GraphNodeError(f'Failed to initialize Table: '
                                      f'incorrect node type {self.ntype} of vertex id = {vid}')

    def __repr__(self):
        return f'Table[{self.nid}]'

    def __str__(self):
        return f'Table[{self.nid}]:{self.key}'

    @property
    def sql(self):
        return self.parent.connection.sql

    @property
    def parent(self):
        return self.in_nodes[0]

    @property
    def fields(self):
        """
        :return: shortcut for SchemaPipe operations to output metadata in a dataframe
        """
        return self.get_fields().over('nid, dim3, dim2, cname, ntype, ctype, counter, attributes').\
            to_dataframe('dim3, dim2').out()

    def get_fields(self, mapped=None):
        """
        wrapper for SchemaPipe.get_fields() method
        :param mapped: if True return ONLY those fields that are mapped onto attributes
                       default return all fields
        :return: result from `get_fields` method that can be chained to other operations e.g. over(), out()
        use out() at the end of the chained methods to retrieve the final result
        """
        return self.spipe.get_fields(mapped=mapped)

    def get_rows(self, npartitions=None, partition_size=None):
        """
        wrapper for DataPipe.get_rows() method
        :return: result from `get_rows` method that can be chained to other operations
        use out() at the end of the chained methods to retrieve the final result
        """
        return self.dpipe.get_rows(npartitions, partition_size)

    def get_columns(self):
        """
        wrapper for DataPipe.get_columns() method
        :return:  return result from `get_rows` method that can be chained to other operations
        use out() at the end of the chained methods to retrieve the final result
        """
        return self.dpipe.get_columns()

    def container_metadata(self, **kwargs):
        """
        :return: metadata for the data resource container e.g. metadata for columns of MySQL table
        """
        if self.ctype in db_types:
            kwargs['table'] = self.cname
            return self.parent.connection.get_columns_metadata(**kwargs)
        else:
            raise GraphNodeError(f'Failed, cannot get metadata of dataset with container type {self.ctype}')

    def add_fields(self, metadata=None):
        """
        :param metadata: list of dictionaries,
                         each dictionary contains metadata column properties for a field (column) in a table
        :return: new Field objects
        """
        if metadata is None and self.ctype == 'SQLite':
            # Mapping: SQLite --> HyperMorph properties
            # name --> cname
            # type --> vtype
            # dflt_value --> default
            # notnull --> missing
            column_metadata = self.container_metadata(fields='name, type, notnull, dflt_value', out='tuples')
            fields = [self.add_field(cname=fld_name, ctype=self.ctype,
                                     extra={'vtype': fld_vtype, 'missing': fld_nullable, 'default': fld_default})
                      for fld_name, fld_vtype, fld_nullable, fld_default in column_metadata]

        elif metadata is None and self.ctype == 'MYSQL':
            # Mapping: MYSQL --> HyperMorph properties
            # COLUMN_NAME --> cname
            # COLUMN_TYPE --> vtype
            # COLUMN_DEFAULT --> default
            # IS_NULLABLE --> missing
            # COLLATION_NAME --> collation
            # COLUMN_KEY --> unique
            column_metadata = self.container_metadata(fields='COLUMN_NAME, COLUMN_TYPE, COLUMN_DEFAULT, '
                                                             'IS_NULLABLE, COLLATION_NAME, COLUMN_KEY', out='tuples')
            fields = [self.add_field(cname=fld_name, ctype=self.ctype,
                                     extra={'vtype': fld_vtype, 'default': fld_default, 'missing': fld_nullable,
                                            'collation': fld_collation, 'unique': fld_unique})
                      for fld_name, fld_vtype, fld_default, fld_nullable, fld_collation, fld_unique in column_metadata]

        elif metadata is None and self.ctype == 'CLICKHOUSE':
            # Mapping: CLICKHOUSE --> HyperMorph properties
            # COLUMN_NAME --> cname
            # COLUMN_TYPE --> vtype
            column_metadata = self.container_metadata(fields='name, type', out='tuples')
            fields = [self.add_field(cname=fld_name, ctype=self.ctype, extra={'vtype': fld_vtype})
                      for fld_name, fld_vtype in column_metadata]

        elif metadata is None and self.ctype in flat_file_types:
            field_names = FileUtils.flatfile_header(self.ctype, self.path)
            fields = [self.add_field(cname=fld_name, ctype=self.ctype) for fld_name in field_names]

        elif metadata is None and self.ctype == 'FEATHER':
            schema = FileUtils.feather_to_arrow_schema(self.path)
            schema_vtypes = [t.value_type.__str__() for t in schema.types]
            fields = [self.add_field(cname=fld_name, ctype=self.ctype, extra={'vtype': fld_vtype})
                      for fld_name, fld_vtype in zip(schema.names, schema_vtypes)]

        elif isinstance(metadata, dict):
            fields = [self.add_field(**d) for d in metadata]

        else:
            raise GraphNodeError('Failed to add fields')

        return fields

    def add_field(self, **nprops):
        """
        :param nprops: schema node (vertex) properties
        :return: single Field object
        """
        # Parse parameters
        if 'cname' not in nprops or 'ctype' not in nprops:
            raise GraphNodeError(f'Failed to add field: <cname> and <ctype> parameters are mandatory')

        ctype = nprops['ctype']
        if ctype not in container_types:
            raise GraphNodeError(f'Failed to add field: unknown container type {ctype}')

        try:
            # Set object dimensions and other properties
            dim4 = self.dim4
            dim3 = self.dim3
            dim2 = self.parent.counter+1
            cnt = 0
            node_type = 'FLD'

            # Update node properties
            nprops.update({'counter': cnt, 'dim4': dim4, 'dim3': dim3, 'dim2': dim2, 'ntype': node_type})

            # Create new field
            new_field = Field(self.schema, **nprops)
        except Exception:
            raise GraphNodeError(f'Failed to create Field object')

        # Increment the counter of the Table node
        self.schema.counter[self.vertex] += 1

        # Increment the counter of the DataSet node
        self.schema.counter[self.parent.vertex] += 1

        # link Table (from node - tail) to Field (to node - head), i.e. create edges
        self.schema.add_link(self, new_field, etype=1, ename=5, elabel=5, ealias=5)

        return new_field

    def to_hypergraph(self):
        # wrapper for SchemaPipe.get_fields() method
        return self.spipe.to_hypergraph()
