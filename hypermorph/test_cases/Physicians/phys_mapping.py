"""
HyperMorph Modules Testing:
        Map selected Field(s) (columns) of a FEATHER Table onto Attribute(s) of a NEW DataModel
        Load data from feather file and encode them with dictionaries in a PyArrow Table
        Save the modified Schema and the data of the NEW Associative Entity Set

(C) August 2020 By Athanassios I. Hatzis
"""
from hypermorph import MIS

# Load Schema
mis = MIS(debug=1, load=True, net_name='my_schema_binary', net_format='gt', net_path='/data/test/Schemata')

#
# Examine DataSet
#
# Metadata
phys_dataset = mis.get(413)
phys_dataset.get_components().to_dataframe().out()
phys_table = phys_dataset.get_tables().to_nodes().out()[0]
# Data
projection = 'npi, pacID, profID, groupID, groupTotal, last, first, gender, graduated, city, state, lbn1'
phys_table.get_rows().over(select=projection).slice(limit=10).to_table().to_dataframe().out()


# ======================================================================================================
# Add NEW associative entity
# By mapping selected fields from feather file onto NEW Attributes of a NEW DataModel
# ======================================================================================================
#
fld_names = phys_table.get_fields().over('cname').to_tuples().out()
aliases = [f'_{num+2}' for num, _ in enumerate(fld_names)]
phys = mis.add('aset', from_table=phys_table, with_fields=fld_names,
               datamodel_name='Physician Data Model', datamodel_alias='PHYS_DM',
               entity_name='Physician', entity_alias='PHYS', as_names=aliases)
phys.attributes

# ======================================================================================================
# Load data from feather file and encode them with dictionaries in a PyArrow Table
# ======================================================================================================
phys.dictionary_encode()
phys.attributes
phys.data
phys.hacols
phys.memory_usage(mb=True)
# =======================================================================================================
# Save Modified Schema and the data of Associative Entity Set
# =======================================================================================================
mis.save()

