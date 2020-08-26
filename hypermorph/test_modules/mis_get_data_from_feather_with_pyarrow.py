"""
HyperMorph Modules Testing: Get data from parquet data resource
(C) August 2020 By Athanassios I. Hatzis
"""
from hypermorph import MIS, FileUtils

# New Schema
mis = MIS(debug=1, rebuild=True, warning=False, net_name='schema', net_format='gt', net_path='/data/DemoData/Schemata')

mis.add('dataset', with_components=['tables', 'fields'],
        cname='Physicians Dataset', alias='PHYS', ctype='FEATHER',
        descr='USA Physicians Compare National dataset',
        extra={'path': 'Physicians'})

mis.get(5).components
columns = 'npi, pacID, profID, groupID, groupTotal, last, first, gender, graduated, city, state'
aliases = '_2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12'

tbl = mis.get(6).get_columns().over(select=columns).to_table(arrow_encoding=False).out()
tbl.num_rows
tbl
mis.mem_diff    # 288.7 MB

mis.get(6).get_columns().over(select=columns).to_table().to_dataframe().out()

mis.get(6).get_columns().over(select=columns, as_names=aliases).to_table().out()
mis.mem_diff    # 64.8 MB

mis.get(6).get_columns().over(select=columns).slice(limit=10).to_table().to_dataframe(index='npi, pacID').out()
mis.mem_diff    # 215 MiB

mis.get(6).get_columns().over(select=columns).to_batch().out()

