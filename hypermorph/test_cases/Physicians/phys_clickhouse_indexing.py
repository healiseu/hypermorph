#
# New associative engine using ClickHouse
#
from hypermorph.mis import MIS
from hypermorph.clients import ConnectionPool
from hypermorph.utils import bytes2mb
from hypermorph.utils import FileUtils
import numpy as np
import pyarrow as pa

# from hypermorph import MIS
mis = MIS(debug=1, load=True, net_name='my_schema_binary', net_format='gt', net_path='/data/test/Schemata')

ch = ConnectionPool(db_client='Clickhouse-Driver', host='localhost', port=9000,
                    user='demo', password='demo', database='TestDB', trace=0)

#
# ClickHouse Solution for dictionary encoding
#
fld_names = 'npi, pacID, profID, last, first, gender, school, graduated, spec0, spec1, org, groupID, groupTotal, ' \
            'city, state, phone, ccn1, lbn1, payCode, pqrs, ehr, hearts'.split(', ')
fld_ids = (2, 3, 4, 5, 6, 9, 11, 12, 13, 14, 19, 20, 21, 25, 26, 28, 29, 30, 39, 40, 41, 42)

names, dtypes = ch.get_columns_metadata(table='Physician', columns=fld_names, fields='name, type', out='columns')
name_dtype_dict = dict(zip(names, dtypes))
name_id_dict = dict(zip(fld_names, fld_ids))

id_name_dict = dict(zip(fld_ids, fld_names))
id_dtype_dict = {name_id_dict[key]: name_dtype_dict[key] for key in name_dtype_dict}


def create_clickhouse_index_tables(table_name, column_id):
    # Dictionary for unique secondary index
    ch.connector.create_engine(table=f'DIC_{column_id}', engine='Memory',
                               heading=['ha1 UInt32', f'val {id_dtype_dict[column_id]}'], execute=True)
    ch.sql(f"""
        INSERT INTO DIC_{column_id}
        SELECT rowNumberInAllBlocks() ha1, val FROM
        (SELECT DISTINCT {id_name_dict[column_id]} val FROM {table_name})
        ORDER BY val
        """)

    # Symbol table with indices
    ch.connector.create_engine(table=f'COL_{column_id}', engine='Memory',
                               heading=['hb1 UInt32', 'ha1 Nullable(UInt32)', f'val {id_dtype_dict[column_id]}'],
                               execute=True)
    ch.sql(f"""
        INSERT INTO COL_{column_id}
        SELECT rowno hb1, ha1, val FROM {table_name}
        LEFT OUTER JOIN
        (SELECT ha1, val FROM DIC_{column_id})
        ON {id_name_dict[column_id]}=val
        """)


for ha2 in fld_ids:
    create_clickhouse_index_tables('Physician', ha2)


ch.sql('select rowno from Physician where graduated>2015')
ch.sql('select hb1 from COL_12 where val>2015')


ch.sql("""
select ha1 from COL_11 where hb1 in
(select hb1 from COL_12 where val>2015)
""")

