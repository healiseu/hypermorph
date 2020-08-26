"""
HyperMorph Demo:
                Add an extra data resource (Feather file) to an existing Schema
                Map selected Field(s) (columns) of the feather file onto Attribute(s) of a NEW DataModel
                Load data from feather file and encode them with dictionaries in a PyArrow Table
                Save the modified Schema and the data of the NEW Associative Entity Set
(C) August 2020 By Athanassios I. Hatzis
"""
from hypermorph import MIS

# Load Existing Schema
mis = MIS(debug=1, load=True, net_name='demo_schema', net_format='gt', net_path='data/DemoData/Schemata')

# ====================================================================
# Add an extra data resource (Feather File DataSet) to the existing Schema,
# ====================================================================
# Physicians Feather file
phys_dataset = mis.add('dataset', with_components=['tables', 'fields'],
                       cname='Physicians Dataset', alias='PHYS', ctype='FEATHER',
                       descr='USA Physicians Compare National dataset',
                       extra={'path': 'Physicians'})
# ---------------------
# Examine DataSet
# ---------------------
# Metadata
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
