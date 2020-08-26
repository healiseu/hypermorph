"""
HyperMorph Demo: Add resources to a new Schema
(C) August 2020 By Athanassios I. Hatzis
"""
from hypermorph import MIS, FileUtils

# rebuild TRIADB Schema
mis = MIS(debug=1, rebuild=True, warning=False,
          net_name='demo_schema', net_format='gt', net_path='data/DemoData/Schemata')

# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
# ***************************************************************************************
#                           =======   Add Data Resources =======
# ***************************************************************************************
# \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\

# =====================================================================
# Add Flat File DataSets
# =====================================================================
mis.add('dataset', with_components=['tables', 'fields'], extra={'path': 'SupplierPartCatalog'},
        ctype='TSV', cname='Supplier Part Catalogue TSV flat files', alias='SPC',
        descr='Supplier, Part, Catalog and denormalized data')

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
# Add GraphDataModels
# =====================================================================
#
gdm_dataset = mis.add('dataset', with_components='graph data models', extra={'path': ''},
                      cname='Simple Graph Data Models',  alias='MYGDMS', ctype='GRAPHML',
                      descr='A set of files with <.graphml> format to serialize Data Models')
# View Dataset
mis.get(242, 'components', select='nid, cname, alias, ntype, ctype, path', out='dataframe')
# Load DataModel from GraphDataModel file on Schema
mis.get(243).load_into_schema()


# =====================================================================
# Get an overview of Systems, DataSets and DataModels created
# =====================================================================
mis.overview

# ======================================================================
# Save Schema
# ======================================================================
mis.save()
