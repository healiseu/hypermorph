"""
HyperMorph Modules Testing: Transform a flat file to a PyArrow table and Pandas dataframe
(C) August 2020 By Athanassios I. Hatzis
"""
from hypermorph.utils import FileUtils

file_location = '/data/test/FlatFiles/SupplierPartCatalog/spc_catalog.tsv'

sep = '\t'
nulls = ['\\N']
skip_rows = 1

select = 'sid, pid, price, quantity, inspection, check'.split(', ')

# Issue with the column_names of the read_options (OMIT) see Arrow JIRA
# https://issues.apache.org/jira/browse/ARROW-9522#
as_names = 'sid, pid, price, quantity, inspection, check, OMIT'.split(', ')

as_types = 'uint32, uint32, float32, uint32, timestamp[ms], bool'.split(', ')

FileUtils.flatfile_to_pyarrow_table(file_type='TSV', file_location=file_location,
                                    select=select, as_columns=as_names, as_types=as_types,
                                    skip=skip_rows, nulls=nulls, delimiter=sep).to_pandas()


select = 'catsid, catpid, catcost, catqnt'
FileUtils.flatfile_to_pandas_dataframe(file_type='TSV', file_location=file_location, nulls=nulls, select=select)

select = 'sid, pid, price, quantity, inspection, check'
as_names = 'sid, pid, price, quantity, inspection, check'
FileUtils.flatfile_to_pandas_dataframe(file_type='TSV', file_location=file_location, nulls=nulls,
                                       select=select, as_columns=as_names, offset=1, index='sid, pid')


"""
     sid  pid       price  quantity inspection  check
0   1081  991   36.099998     300.0 2014-12-20   True
1   1081  992   42.299999     400.0 2014-12-20   True
2   1081  993   15.300000     200.0 2014-03-03  False
3   1081  994   20.500000     100.0 2014-03-03  False
4   1081  995   20.500000       NaN        NaT  False
5   1081  996  124.230003       NaN        NaT  False
6   1081  997  124.230003       NaN        NaT  False
7   1081  998   11.700000     400.0 2014-09-10   True
8   1081  999   75.199997       NaN        NaT  False
9   1082  991   16.500000     200.0 2014-09-10   True
10  1082  997    0.550000     100.0 2014-09-10   True
11  1082  998    7.950000     200.0 2014-03-03  False
12  1083  998   12.500000       NaN        NaT  False
13  1083  999    1.000000       NaN        NaT  False
14  1084  994   57.299999     100.0 2014-12-20   True
15  1084  995   22.200001       NaN        NaT  False
16  1084  998   48.599998     200.0 2014-12-20   True
"""