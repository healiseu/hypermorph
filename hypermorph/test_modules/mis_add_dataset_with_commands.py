"""
HyperMorph Modules Testing:
    adding dataset, tables, and fields
(C) August 2020 By Athanassios I. Hatzis
"""
from hypermorph import MIS

# rebuild Schema
mis = MIS(debug=1, rebuild=True, net_name='user_schema', net_format='graphml', net_path='/data/test/Schemata')

#
# =====================================================================
# Add MySQL database
# =====================================================================

#
# Supplier-Part-Catalog MySQL database
#

# Add dataset
mis.add('dataset', with_components=['tables', 'fields'],
        cname='Supplier Part Catalogue in MySQL', alias='SPC', ctype='MYSQL',
        extra={'host': 'localhost', 'port': 3306,
               'user': 'demo', 'password': 'demo', 'db': 'SPC', 'api': 'MYSQL-Connector'},
        descr='It is a toy database with three tables that form a many to many relationship ')

#
# Physicians Feather file
#
phys_dataset = mis.add('dataset', with_components=['tables', 'fields'],
                       cname='Physicians Dataset', alias='PHYS', ctype='FEATHER',
                       descr='USA Physicians Compare National dataset',
                       extra={'path': 'Physicians'})
