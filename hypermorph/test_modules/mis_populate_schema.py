"""
HyperMorph Modules Testing:
    rebuilding schema and start populating with DataSets and DataModels
    get an overview of Systems, DataSets and DataModels
    save Schema Graph in a file
(C) August 2020 By Athanassios I. Hatzis
"""
from hypermorph import MIS

# rebuild Schema
mis = MIS(debug=1, rebuild=True, warning=False,
          net_name='my_schema_binary', net_format='gt', net_path='/data/test/Schemata')

# =====================================================================
# Add SQLite databases
# =====================================================================
#
# Supplier-Part-Catalog SQLite database
mis.add('dataset', with_components=['tables', 'fields'], extra={'path': 'spc.sqlite', 'api': 'SQLAlchemy'},
        ctype='SQLite', cname='Supplier Part Catalogue in SQLite', alias='SPC',
        descr='It is a toy database with three tables that form a many to many relationship ')

# Northwind Traders SQLite database
mis.add('dataset', with_components=['tables', 'fields'], extra={'path': 'northwind.sqlite', 'api': 'SQLAlchemy'},
        ctype='SQLite', cname='Northwind Traders', alias='NORTHWIND',
        descr='Northwind Traders Access database is a sample database that shipped with Microsoft Office')


# =====================================================================
# Add MySQL databases
# =====================================================================
#
# Supplier-Part-Catalog MySQL database
mis.add('dataset', with_components=['tables', 'fields'],
        cname='Supplier Part Catalogue in MySQL', alias='SPC', ctype='MYSQL',
        extra={'host': 'localhost', 'port': 3306, 'user': 'demo', 'password': 'demo',
               'db': 'SPC', 'api': 'SQLAlchemy'},
        descr='It is a toy database with three tables that form a many to many relationship ')

# Northwind Traders MySQL database
mis.add('dataset', with_components=['tables', 'fields'],
        cname='Northwind Traders', alias='NORTHWIND', ctype='MYSQL',
        extra={'host': 'localhost', 'port': 3306, 'user': 'demo', 'password': 'demo',
               'db': 'Northwind', 'api': 'MYSQL-Connector'},
        descr='Northwind Traders Access database is a sample database that shipped with Microsoft Office')

# =====================================================================
# Add Flat File DataSets
# =====================================================================
# BikeTrips_TriaClick_OLD_Engine_SQL CSV flat files
mis.add('dataset', with_components=['tables', 'fields'], extra={'path': 'BikeTrips'},
        cname='Bike Trips Dataset', alias='BIKE', ctype='CSV', descr='about data set....')

# Car data CSV flat file
mis.add('dataset', with_components=['tables', 'fields'], extra={'path': 'Cars'},
        cname='Cars Dataset', alias='CARS', ctype='CSV', descr='a data set with 200 car records')

# Supplier-Part-Catalog TSV flat files
mis.add('dataset', with_components=['tables', 'fields'], extra={'path': 'SupplierPartCatalog'},
        ctype='TSV', cname='Supplier Part Catalogue TSV flat files', alias='SPC',
        descr='Supplier, Part, Catalog and denormalized data')

# Physicians TSV flat files
mis.add('dataset', with_components=['tables', 'fields'], extra={'path': 'Physicians'},
        cname='USA Physician Compare National Dataset', alias='PHYS2016', ctype='TSV',
        descr='Health-care professionals with multiple medicare enrollment records')

# ====================================================================
# Add Feather File DataSets
# ====================================================================
#
# Physicians Feather file
mis.add('dataset', with_components=['tables', 'fields'], extra={'path': 'Physicians'},
        cname='Physicians Dataset', alias='PHYS', ctype='FEATHER',
        descr='USA Physicians Compare National dataset')

# =====================================================================
# Add GraphSchema DataSet
# =====================================================================
# This is a DataSet of schemata (GSHs) that are serialized into `.graphml`, `.gt` files
# GSH, like a Table is a component (child) of a DataSet
# Notice: Do not confuse GraphSchema (GSH) with Schema
gsh_dataset = mis.add('dataset', with_components='graph schemata', extra={'path': ''},
                      cname='HyperMorph Schemata',  alias='SCHEMATA', ctype='SCHEMA',
                      descr='A set of files with either <.graphml> or <.gr> format to serialize Schema')

# =====================================================================
# Add GraphDataModel DataSet
# =====================================================================
# For example we can have a DataSet of graph data models (GDMs) that are serialized into `.graphml`, `.gt` files
#
# see < mis.add_datamodel_with_commands.py > example to test how to
# create and write data model into a graphml, gt file
#
# Notice:
# Do not confuse adding a set of GraphDataModels (GDMs), i.e. a set of data resources with
# loading any of these into  Schema in memory.
#
# The last one is a different operation, it creates new DataModels into Schema
#  i.e. loads metadata information about the DataModel, Entities and Attributes into  Schema
#
gdm_dataset = mis.add('dataset', with_components='graph data models', extra={'path': ''},
                      cname='Simple Graph Data Models',  alias='MYGDMS', ctype='GRAPHML',
                      descr='A set of files with <.graphml> format to serialize Data Models')

# =====================================================================
# Load GraphDataModels (see notice above)
# =====================================================================
#
# View Dataset
mis.get(440, 'components', select='nid, cname, alias, ntype, ctype, path', out='dataframe')

# Load GraphDataModels from files into Schema in memory
for gdm in mis.get(440, 'graph_datamodels', out='nodes'):
    print(gdm.load_into_schema())


# =====================================================================
# Get an overview of Systems, DataSets and DataModels created
# =====================================================================
mis.overview

# ======================================================================
# Save Schema
# ======================================================================
mis.save()
