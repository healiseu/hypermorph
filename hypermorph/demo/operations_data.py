"""
HyperMorph Demo:
    Data operations

(C) August 2020 By Athanassios I. Hatzis
"""
from hypermorph import MIS

# Load schema
mis = MIS(debug=1, load=True, net_name='demo_schema', net_format='gt', net_path='data/DemoData/Schemata')

# review schema
mis.overview

# get tables for MySQL dataset
mis.get(136).tables

# get all rows to a dataframe
mis.get(139).get_rows().over().to_dataframe().out()

# get 12 rows with projection to a dataframe
mis.get(139).get_rows().over('catsid, catpid, catqnt, catchk').slice(12).to_dataframe(index='catsid, catpid').out()

# get same 12 rows, from a TSV data resource
mis.get(6).get_rows().over('catsid, catpid, catqnt, catchk').slice(12).to_dataframe(index='catsid, catpid').out()

# get same 12 rows, from SQLite data resource
mis.get(28).get_rows().over('catsid, catpid, catqnt, catchk').slice(12).to_dataframe(index='catsid, catpid').out()

# get 12 rows with projection to tuples
mis.get(139).get_rows().over('catsid, catpid, catqnt, catchk').slice(12).to_tuples().out()

# get 12 rows with projection to pyarrow table with dictionary encoding and rename columns
mis.get(139).get_rows().over('catsid, catpid, catqnt, catchk', as_names='sid, pid, qnt, chk').slice(12).to_table(arrow_encoding=True).out()
