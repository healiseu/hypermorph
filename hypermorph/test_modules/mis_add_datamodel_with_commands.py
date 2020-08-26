"""
HyperMorph Modules Testing:
    adding datamodel, entities, and attributes with the `add` command to a Graph Data Model (GDM)
    Then it saves GDM serialized into `.graphml`, `.gt` files
    If there is an existing Schema you can load GDM with the `load_into_schema` method (see mis.populate test)
    or you can inspect GDM on its own Schema
    e.g. Schema(load=True, net_name='spc_model', net_format='graphml', net_path='/data/test/DataModels')
    (see `schema_load_datamodel` test)
(C) August 2020 By Athanassios I. Hatzis
"""
from hypermorph import MIS

mis = MIS(debug=1, rebuild=True, net_name='spc_model', net_format='graphml', net_path='/data/test/DataModels')

# Add datamodel
dm = mis.add('datamodel', cname='Supplier Part Catalog', alias='SPC',
             descr='Model with three entities that represent relations (tables) in a relational database')

# Add Entities one by one the functional way
mis.add('entity', datamodel=dm, cname='Supplier', alias='SUP', descr='The Supplier Entity of the data model')
mis.add('entity', datamodel=dm, cname='Part',     alias='PRT', descr='The Part Entity of the data model')
mis.add('entity', datamodel=dm, cname='Catalog',  alias='CAT', descr='The Catalog Entity of the data model')

# Add Entities one by one with dot operation
dm.add_entity(cname='Supplier', alias='SUP2', descr='The Supplier Entity of the data model')
dm.add_entity(cname='Part',     alias='PRT2', descr='The Part Entity of the data model')
dm.add_entity(cname='Catalog',  alias='CAT2', descr='The Catalog Entity of the data model')

# Add Entities with a metadata list container
#
entities_metadata = [
    {'cname': 'Catalog',  'alias': 'CAT3', 'descr': 'The Catalog Entity of the data model'},
    {'cname': 'Part',     'alias': 'PRT3', 'descr': 'The Part Entity of the data model'},
    {'cname': 'Supplier', 'alias': 'SUP3', 'descr': 'The Supplier Entity of the data model'}
]
mis.add('entities', datamodel=dm, metadata=entities_metadata)

# Add Attributes one by one
mis.add('attribute', datamodel=dm, entalias='SUP', cname='sname',    alias='s_name',    descr='supplier name',
        extra={'vtype': 'utf8', 'missing': 0})
mis.add('attribute', datamodel=dm, entalias='SUP', cname='saddress', alias='s_address', descr='supplier address',
        extra={'vtype': 'utf8', 'missing': 1})
mis.add('attribute', datamodel=dm, entalias='SUP', cname='scity',    alias='s_city',    descr='supplier city',
        extra={'vtype': 'utf8', 'enum': 1, 'missing': 1})
mis.add('attribute', datamodel=dm, entalias='SUP', cname='scountry', alias='s_country', descr='supplier country',
        extra={'vtype': 'utf8', 'enum': 1, 'missing': 1})
mis.add('attribute', datamodel=dm, entalias='SUP', cname='sstatus',  alias='s_status',  descr='supplier status',
        extra={'vtype': 'bool_', 'enum': 1, 'missing': 1})

dm.add_attribute(entalias='PRT', cname='pname',    alias='p_name',    descr='part name',   extra={'vtype': 'utf8', 'missing': 0})
dm.add_attribute(entalias='PRT', cname='pcolor',   alias='p_color',   descr='part color',  extra={'vtype': 'utf8', 'enum': 1, 'missing': 1})
dm.add_attribute(entalias='PRT', cname='pweight',  alias='p_weight',  descr='part weight', extra={'vtype': 'float32', 'missing': 1})
dm.add_attribute(entalias='PRT', cname='punit',    alias='p_unit',    descr='part unit',   extra={'vtype': 'utf8', 'enum': 1, 'missing': 1})

mis.add('attribute', datamodel=dm, entalias='CAT', cname='cprice',    alias='c_price',    descr='catalog price',
        extra={'vtype': 'float32', 'missing': 0})
mis.add('attribute', datamodel=dm, entalias='CAT', cname='cquantiy',  alias='c_quantity', descr='catalog quantity',
        extra={'vtype': 'uint16', 'missing': 1})
mis.add('attribute', datamodel=dm, entalias='CAT', cname='cdate',     alias='c_date',     descr='catalog date',
        extra={'vtype': 'date32', 'missing': 1})
mis.add('attribute', datamodel=dm, entalias='CAT', cname='ccheck',    alias='c_check',    descr='catalog check',
        extra={'vtype': 'bool_', 'enum': 1, 'missing': 0})

# Add junction Nodes
# Junction nodes are Attributes that are linked to more than one Entities
mis.add('attribute', datamodel=dm, entalias=['SUP', 'CAT'], cname='sid',  alias='s_ID',  descr='supplier id',
        extra={'vtype': 'uint16', 'missing': 0, 'unique': 1})
mis.add('attribute', datamodel=dm, entalias=['PRT', 'CAT'], cname='pid',  alias='p_ID',  descr='part id',
        extra={'vtype': 'uint16', 'missing': 0, 'unique': 1})

# Save the schema in a graph file format (.graphml)
mis.save()
