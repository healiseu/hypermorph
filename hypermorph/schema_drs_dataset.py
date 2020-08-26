

from .clients import ConnectionPool
from .utils import FileUtils
from . import conn_meta, flat_file_types, file_types, container_types, db_types, db_clients
from .schema_node import SchemaNode
from .schema_drs_graph_datamodel import GraphDataModel
from .schema_drs_table import Table
from .exceptions import GraphNodeError


class DataSet(SchemaNode):
    """
    DataSet is a set of data resources (tables, fields, graph datamodels) in the following data containers
    SQLite database, MySQL database, CSV/TSV flat files and graph data files

    Notice: get_* methods return SchemaPipe, DataPipe objects
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

        # Existing DataSet node case
        if vid is not None:
            # Check node type
            if self.ntype != 'DS':
                raise GraphNodeError(f'Failed to initialize DataSet: '
                                     f'incorrect node type {self.ntype} of vertex id = {vid}')
        # Initialize connection
        self._cnx = None

    def __repr__(self):
        return f'DataSet[{self.nid}]'

    def __str__(self):
        return f'DataSet[{self.nid}]:{self.key}'

    @property
    def parent(self):
        return self.in_nodes[0]

    @property
    def components(self):
        """
        :return: shortcut for SchemaPipe operations to output metadata in a dataframe
        """
        return self.get_components().over('nid, dim4, dim3, dim2, cname, alias, ctype, vtype, counter').\
            to_dataframe('dim4, dim3, dim2').out()

    def get_components(self):
        """
        :return: result from  `get_components` method that can be chained to other operations e.g. over(), out()
        use out() at the end of the chained methods to retrieve the final result
        """
        return self.spipe.get_components()

    @property
    def graph_datamodels(self):
        """
        :return: shortcut for SchemaPipe operations to output metadata in a dataframe
        """
        return self.get_graph_datamodels().over('nid, dim3, dim2, cname, ntype, ctype, counter').\
            to_dataframe('dim3, dim2').out()

    def get_graph_datamodels(self):
        """
        :return: result from  `get_graph_datamodels` method that can be chained to other operations e.g. over(), out()
        use out() at the end of the chained methods to retrieve the final result
        """
        # If dataset is container of type `GRAPHML`
        if self.ctype == 'GRAPHML':
            return self.spipe.get_graph_datamodels()
        else:
            raise GraphNodeError(f'Failed to fetch graph datamodels for object {self} with ctype {self.ctype}')

    @property
    def graph_schemata(self):
        """
        :return: shortcut for SchemaPipe operations to output metadata in a dataframe
        """
        return self.get_graph_schemata().over('nid, dim3, dim2, cname, ntype, ctype, counter').\
            to_dataframe('dim3, dim2').out()

    def get_graph_schemata(self):
        """
        :return: result from `get_graph_schemata` method that can be chained to other operations e.g. over(), out()
        use out() at the end of the chained methods to retrieve the final result
        """
        # If dataset is container of type `SCHEMA`
        if self.ctype == 'SCHEMA':
            return self.spipe.get_graph_schemata()
        else:
            raise GraphNodeError(f'Failed to fetch graph schemata for object {self} with ctype {self.ctype}')

    @property
    def tables(self):
        """
        :return: shortcut for SchemaPipe operations to output metadata in a dataframe
        """
        return self.get_tables().over('nid, dim3, dim2, cname, ntype, ctype, counter').\
            to_dataframe('dim3, dim2').out()

    def get_tables(self):
        """
        :return: result from  `get_tables` method that can be chained to other operations e.g. over(), out()
        use out() at the end of the chained methods to retrieve the final result
        """
        return self.spipe.get_tables()

    @property
    def fields(self):
        """
        :return: shortcut for SchemaPipe operations to output metadata in a dataframe
        """
        return self.get_fields().over('nid, dim3, dim2, cname, ntype, ctype, counter, attributes').\
            to_dataframe('dim3, dim2').out()

    def get_fields(self, mapped=None):
        """
        :param mapped: if True return ONLY those fields that are mapped onto attributes
                       default return all fields
        :return: result from  `get_fields` method that can be chained to other operations e.g. over(), out()
        use out() at the end of the chained methods to retrieve the final result
        """
        return self.spipe.get_fields(mapped=mapped)

    def get_connection(self, db_client=None, port=None, trace=0):
        """
        :param db_client:
        :param port: use port for either HTTP or native client connection to clickhouse
        :param trace:
        :return:
        """
        # Create a new connection only the first time @property connection is called
        # and only for the db clients supported
        if self._cnx is None:
            if db_client is not None:
                self.api = db_client

            if port is not None:
                self.port = port

            if self.api not in db_clients:
                raise GraphNodeError(f'Failed to connect: unknown db_client API {self.api}')

            # SQLAlchemy API cases
            if self.api == 'SQLAlchemy':
                if self.ctype == 'SQLite':
                    self._cnx = ConnectionPool(db_client=self.api, dialect='sqlite', path=self.path, trace=trace)
                elif self.ctype == 'MYSQL':
                    self._cnx = ConnectionPool(db_client=self.api, dialect='pymysql', database=self.db, trace=trace,
                                               host=self.host, port=self.port, user=self.user, password=self.password)
                elif self.ctype == 'CLICKHOUSE':
                    self._cnx = ConnectionPool(db_client=self.api, dialect='clickhouse', database=self.db, trace=trace,
                                               host=self.host, port=self.port, user=self.user, password=self.password)
            # MySQL-Connector API, ClickHouse API
            elif self.api in ['Clickhouse-Driver', 'MYSQL-Connector']:
                self._cnx = ConnectionPool(db_client=self.api, database=self.db, trace=trace,
                                           host=self.host, port=self.port, user=self.user, password=self.password)
        return self._cnx

    @property
    def connection(self):
        if not self._cnx:
            self._cnx = self.get_connection()
        return self._cnx

    @property
    def connection_metadata(self):
        return {col: self.get_value(col) for col in conn_meta}

    def container_metadata(self, **kwargs):
        """
        :return: metadata for the data resource container
                 e.g. metadata for a parquet file, or the tables of a database
        """
        # tables metadata for database data resource
        if self.ctype in db_types:
            return self.connection.get_tables_metadata(**kwargs)
        else:
            raise GraphNodeError(f'Failed, cannot get metadata of dataset with container type {self.ctype}')

    def add_fields(self):
        """
        Structure here is hierarchical
        a    DataSet ---has---> Tables
        each Table   ---has---> Fields

        :return: new Field objects
        """
        return [tbl.add_fields() for tbl in self.out_nodes]

    @staticmethod
    def _get_extension(ctype):
        if ctype == 'CSV':
            return 'csv'
        elif ctype == 'TSV':
            return 'tsv'
        elif ctype == 'PARQUET':
            return 'pq'
        elif ctype == 'FEATHER':
            return 'fr'

    def add_tables(self, metadata=None):
        """
        :param metadata: list of dictionaries,
                         keys of dictionary are metadata property names of Table node
        :return: new Table objects
        """
        # Add tables automatically from Database tables or CSV/TSV files
        if metadata is None and self.ctype == 'SQLite':
            table_names = self.container_metadata(fields='name', out='tuples')
            tables = [self.add_table(cname=tbl_name[0], ctype=self.ctype) for tbl_name in table_names]
        elif metadata is None and self.ctype == 'MYSQL':
            table_names = self.container_metadata(fields='TABLE_NAME', out='tuples')
            tables = [self.add_table(cname=tbl_name[0], ctype=self.ctype) for tbl_name in table_names]
        elif metadata is None and self.ctype == 'CLICKHOUSE':
            table_names = self.container_metadata(fields='name', out='tuples')
            tables = [self.add_table(cname=tbl_name[0], ctype=self.ctype) for tbl_name in table_names]
        elif metadata is None and self.ctype in file_types:
            table_iter = [(fp.stem, fp) for fp in self.path.glob(f'*.{self._get_extension(self.ctype)}')]
            tables = [self.add_table(cname=tbl_name, ctype=self.ctype, extra={'path': tbl_path})
                      for tbl_name, tbl_path in table_iter]

        # Add tables with a command
        elif isinstance(metadata, list):
            tables = [self.add_table(**d) for d in metadata]
        else:
            raise GraphNodeError('Failed to add tables')

        return tables

    def add_table(self, **nprops):
        """
        :param nprops: schema node (vertex) properties
        :return: single Table object
        """
        # Parse parameters
        if 'cname' not in nprops or 'ctype' not in nprops:
            raise GraphNodeError(f'Failed to add table: <cname> and <ctype> parameters are mandatory')

        ctype = nprops['ctype']
        if ctype not in container_types:
            raise GraphNodeError(f'Failed to add table: unknown container type {ctype}')

        try:
            # Set object dimensions and other properties
            dim4 = self.dim4
            dim3 = self.dim3
            dim2 = self.get_value('counter') + 1
            cnt = 0
            node_type = 'TBL'

            # Update node properties
            nprops.update({'counter': cnt, 'dim4': dim4, 'dim3': dim3, 'dim2': dim2, 'ntype': node_type})

            # Create new table
            new_table = Table(self.schema, **nprops)
        except Exception:
            raise GraphNodeError(f'Failed to create Table object')

        # Increment the counter of the DataSet node
        self.schema.counter[self.vertex] += 1

        # link DataSet (from node - tail) to Table (to node - head), i.e. create edges
        self.schema.add_link(self, new_table, etype=1, ename=4, elabel=4, ealias=4)

        return new_table

    def add_graph_schemata(self):
        """
        Add graph schemata

        :return: new GSH objects
        """
        # Currently only serialized GraphSchema (GSH) is supported with .graphml or .gr file format
        # that can be read from graph-tool
        if self.ctype == 'SCHEMA':
            lis = [(fp.stem, fp) for fp in self.path.glob(f'*.*')]
            gshs = [self.add_graph_schema(cname=file_name, ctype=self.ctype, extra={'path': file_path})
                    for file_name, file_path in lis]
        else:
            raise GraphNodeError(f'Failed to add GraphDataModels, wrong type of dataset container <{self.ctype}>')

        return gshs

    def add_graph_schema(self, **nprops):
        """
        Add graph schema, this is a graph serialization of HyperMorph Schema

        :param nprops: schema node (vertex) properties
        :return: single GSH object
        """
        # Parse parameters
        if 'cname' not in nprops or 'ctype' not in nprops:
            raise GraphNodeError(f'Failed to add graph schema: <cname> and <ctype> parameters are mandatory')

        ctype = nprops['ctype']
        if ctype not in container_types:
            raise GraphNodeError(f'Failed to add graph schema: unknown container type {ctype}')

        try:
            # Set object dimensions and other properties
            dim4 = self.dim4
            dim3 = self.dim3
            dim2 = self.get_value('counter') + 1
            cnt = 0
            node_type = 'GSH'

            # Update node properties
            nprops.update({'counter': cnt, 'dim4': dim4, 'dim3': dim3, 'dim2': dim2, 'ntype': node_type})

            # Create new table
            new_gsh = GraphDataModel(self.schema, **nprops)
        except Exception:
            raise GraphNodeError(f'Failed to create GraphSchema object')

        # Increment the counter of the DataSet node
        self.schema.counter[self.vertex] += 1

        # link DataSet (from node - tail) to Graph DataModel (to node - head), i.e. create edges
        self.schema.add_link(self, new_gsh, etype=1, ename=11, elabel=11, ealias=11)

        return new_gsh

    def add_graph_datamodels(self):
        """
        Add graph data models

        :return: new GDM objects
        """
        # Currently only serialized GraphDataModels (GDMs) are supported with .graphml or .gt file format
        # that can be read from graph-tool
        if self.ctype == 'GRAPHML' or self.ctype == 'GT':
            lis = [(fp.stem, fp) for fp in self.path.glob(f'*.*')]
            gdms = [self.add_graph_datamodel(cname=file_name, ctype=self.ctype, extra={'path': file_path})
                    for file_name, file_path in lis]
        else:
            raise GraphNodeError(f'Failed to add GraphDataModels, wrong type of dataset container <{self.ctype}>')

        return gdms

    def add_graph_datamodel(self, **nprops):
        """
        Add graph data model, this is a graph serialization of TRIADB data model

        :param nprops: schema node (vertex) properties
        :return: single GDM object
        """
        # Parse parameters
        if 'cname' not in nprops or 'ctype' not in nprops:
            raise GraphNodeError(f'Failed to add graph data model: <cname> and <ctype> parameters are mandatory')

        ctype = nprops['ctype']
        if ctype not in container_types:
            raise GraphNodeError(f'Failed to add graph data model: unknown container type {ctype}')

        try:
            # Set object dimensions and other properties
            dim4 = self.dim4
            dim3 = self.dim3
            dim2 = self.get_value('counter') + 1
            cnt = 0
            node_type = 'GDM'

            # Update node properties
            nprops.update({'counter': cnt, 'dim4': dim4, 'dim3': dim3, 'dim2': dim2, 'ntype': node_type})

            # Create new table
            new_gdm = GraphDataModel(self.schema, **nprops)
        except Exception:
            raise GraphNodeError(f'Failed to create GraphDataModel object')

        # Increment the counter of the DataSet node
        self.schema.counter[self.vertex] += 1

        # link DataSet (from node - tail) to Graph DataModel (to node - head), i.e. create edges
        self.schema.add_link(self, new_gdm, etype=1, ename=6, elabel=6, ealias=6)

        return new_gdm
