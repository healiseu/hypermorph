"""
HyperMorph Modules Testing:
    Transform a Table node from a DataSet to an Entity of a DataModel

(C) August 2020 By Athanassios I. Hatzis
"""

from hypermorph import MIS

# Load Schema
mis = MIS(debug=1, load=True, net_name='my_schema_binary', net_format='gt', net_path='/data/test/Schemata')

# Map Table to a new Entity and selected fields onto new attributes
mis.get(414).\
    get_fields().take(select='npi, pacID, profID, last, first, gender, graduated, city, state, pqrs, ehr, hearts').\
    to_entity(cname='Physician', alias='Phys').out()

# Verify mapping
mis.get(481).attributes
mis.get(414).fields
mis.get(414).get_fields(mapped=True).over('nid, dim4, dim3, dim2, cname, alias, vtype, attributes').to_dataframe().out()
mis.get(414).get_fields(mapped=False).over('nid, dim4, dim3, dim2, cname, alias, attributes').to_dataframe().out()

# Map selected fields onto existing attributes of a datamodel
mis.get(329).get_fields().\
    take(select='FLD2, FLD3, FLD4, FLD5, FLD6, FLD9, FLD12, FLD25, FLD26, FLD40, FLD41, FLD42').\
    to_entity(datamodel=mis.get(480), select='npi, pacID, profID, last, first, gender, graduated, city, state, pqrs, ehr, hearts').out()

# Verify mapping
mis.get(481).attributes
mis.get(329).fields
mis.get(329).get_fields(mapped=True).over('nid, dim4, dim3, dim2, cname, alias, vtype, attributes').to_dataframe().out()
