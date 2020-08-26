"""
This file is part of HyperMorph operational API for information management and data transformations
on Associative Semiotic Hypergraph Development Framework
(C) 2015-2019 Athanassios I. Hatzis

HyperMorph is free software: you can redistribute it and/or modify it under the terms of
the GNU Affero General Public License v.3.0 as published by the Free Software Foundation.

HyperMorph is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with HyperMorph.
If not, see <https://www.gnu.org/licenses/>.
"""

__version__ = '0.1.2'
__version_update__ = '2020-08-25'
__release_version__ = '0.1.2'
__release_date__ = ''
__source_url__ = ''
__python_version__ = '>=3.8.0'
__author__ = 'Athanassios I. Hatzis'
__author_email = 'hatzis@healis.eu'
__organization__ = "HEALIS (Healthy Information Systems/Services)"
__organization_url = "https://healis.eu"
__copyright__ = "Copyright (c) 2015-2020 Athanassios I. Hatzis"
__license__ = "GNU AGPL v3.0"
__distributor__ = 'Promoted and Distributed by HEALIS'
__distributor_url__ = 'https://healis.eu'
__maintainer__ = 'Athanassios I. Hatzis'
__maintainer_email__ = 'hatzis@healis.eu'
__status__ = 'Alpha'


vp_names = ['nid', 'dim4', 'dim3', 'dim2', 'counter', 'ntype', 'ctype',  'cname', 'alias', 'descr', 'extra']
calculated_properties = ['key', 'parent', 'entities', 'attributes', 'fields']

# `field_meta` and `conn_meta` are keys of the `extra` data catalog node (vertex) property
field_meta = ['vtype', 'enum', 'default', 'missing', 'unique', 'collation', 'junction']
conn_meta = ['path', 'db', 'host', 'port', 'user', 'password', 'api']
# Descriptive information
descr_meta = ['cname', 'alias', 'descr']
# System information
sys_meta = ['nid', 'dim4', 'dim3', 'dim2', 'counter', 'ntype', 'ctype']
#
# See also the decorator properties:
# Field.metadata, DataSet.connection_metadata, SchemaNode.descriptive_metadata, SchemaNode.system_metadata
#

file_types = ['CSV', 'TSV', 'FEATHER', 'PARQUET', 'SQLite', 'GRAPHML', 'GT']
flat_file_types = ['CSV', 'TSV']
container_types = ['SQLite', 'MYSQL', 'CLICKHOUSE', 'CSV', 'TSV', 'GRAPHML', 'GT', 'FEATHER', 'SCHEMA']

db_types = ['MYSQL', 'CLICKHOUSE', 'SQLite']
sqlalchemy_dialects = ['pymysql', 'clickhouse', 'sqlite']
db_clients = ['Clickhouse-Driver', 'MYSQL-Connector', 'SQLAlchemy']


node_types = ['SYS', 'DS', 'DM', 'GDM', 'GSH', 'TBL', 'FLD', 'ENT', 'ATTR']

out_types = ['nodes', 'vertices', 'nids', 'keys', 'dict',
             'arrays', 'tuples', 'json_rows', 'named_tuples', 'dataframe', 'table', 'columns']

what_pipe_types = ['components', 'entities', 'attributes', 'tables', 'fields', 'graph_datamodels', 'graph_schemata']

what_types = ['node', 'data', 'container_metadata',
              'all_metadata', 'field_metadata', 'descriptive_metadata', 'connection_metadata', 'system_metadata']


from .schema import Schema
from .mis import MIS
from .utils import FileUtils
