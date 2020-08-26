"""
HyperMorph Modules Testing:
    load datamodel from a file with .graphml format and inspect entities and attributes
(C) August 2020 By Athanassios I. Hatzis
"""
from hypermorph.schema import Schema

# Load data model
spc_schema = Schema(load=True, net_name='spc_model', net_format='graphml', net_path='/data/test/DataModels')

spc_schema.get(5).get_components().to_dataframe().out()

spc_schema.get(5).entities

spc_schema.get(5).attributes
spc_schema.get(5).get_attributes().over('nid, dim4, dim3, dim2, cname, alias, ntype, entities, '
                                        'vtype, enum, missing, unique, junction').to_dataframe().out()

spc_schema.get(6).attributes
spc_schema.get(7).attributes
spc_schema.get(8).attributes
