#
# New associative engine using ClickHouse
#

from hypermorph.clients import ConnectionPool
from hypermorph.utils import bytes2mb
import numpy as np
import pyarrow as pa
import pandas as pd


def create_clickhouse_index_tables(table_name, column_id):
    # Dictionary for unique secondary index
    hb2 = int(table_name[-1:])
    column_dtype = meta[hb2]['type'][column_id]
    column_name = meta[hb2]['name'][column_id]

    ch.connector.create_engine(table=f'DIC_{column_id}_{hb2}', engine='Memory',
                               heading=['ha1 UInt32', f'val {column_dtype}'], execute=True)
    ch.sql(f"""
        INSERT INTO DIC_{column_id}_{hb2}
        SELECT rowNumberInAllBlocks() ha1, val FROM
        (SELECT DISTINCT {column_name} val FROM {table_name})
        ORDER BY val
        """)

    # Symbol table with indices
    ch.connector.create_engine(table=f'COL_{column_id}_{hb2}', engine='Memory',
                               heading=['hb1 UInt32', 'ha1 Nullable(UInt32)', f'val {column_dtype}'],
                               execute=True)
    ch.sql(f"""
        INSERT INTO COL_{column_id}_{hb2}
        SELECT rowno hb1, ha1, val FROM {table_name}
        LEFT OUTER JOIN
        (SELECT ha1, val FROM DIC_{column_id}_{hb2})
        ON {column_name}=val
        """)


# Construct ClickHouse Tables
ch = ConnectionPool(db_client='Clickhouse-Driver', host='localhost', port=9000,
                    user='demo', password='demo', database='SPC_DB', trace=0)
df = ch.get_columns_metadata(table='DAT_242_1', fields='name, type')
meta = {1: df[1:7].set_index(pd.Index([16, 17, 18, 14, 13, 15])).sort_index()}

for ha2 in meta[1].index:
    create_clickhouse_index_tables('DAT_242_1', ha2)


# ===================================================================================================
# Queries
ch.sql('select * from DAT_242_1 where c_price<20')
ch.sql('SELECT * FROM COL_13_1 WHERE val < 20')

# Get rows (hb1) from ClickHouse
flt1 = ch.sql('SELECT hb1 FROM COL_13_1 WHERE val < 20', out='arrow', dictionary_encoding=False)
