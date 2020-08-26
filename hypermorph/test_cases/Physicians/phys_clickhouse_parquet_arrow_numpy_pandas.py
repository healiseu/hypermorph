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
            'city, state, phone, ccn1, lbn1, payCode, pqrs, ehr, hearts'
fld_ids = (2, 3, 4, 5, 6, 9, 11, 12, 13, 14, 19, 20, 21, 25, 26, 28, 29, 30, 39, 40, 41, 42)

# Get metadata for ClickHouse columns
ch.get_columns_metadata(table='Physician', columns=fld_names.split(', '), fields='name, type')

# ClickHouse to PyArrow RecordBatch
phys_record_batch = ch.sql(f'select {fld_names} from Physician', out='arrow')
# Wall time: 22.4 s
mis.mem_diff
bytes2mb(phys_record_batch.nbytes)
# 237 MB

# zero copy transformation from record batch to table
phys_tbl = pa.Table.from_arrays(phys_record_batch.columns, names=phys_record_batch.schema.names)

# Write to disk with parquet format
# FileUtils.arrow_table_to_parquet(phys_tbl, '/data/test/Parquet/Physicians_22cols.parquet')
# !ls -lh /data/test/Parquet/Physicians_22cols*
# -rw-rw-r-- 1 athan athan 85M Jul  8 23:37 /data/test/Parquet/Physicians_22cols.parquet

# Read parquet from disk
phys = FileUtils.parquet_to_arrow_table(file_location='/data/test/Parquet/Physicians_22cols.parquet')
# Wall time: 659 ms
mis.mem_diff


#
# Filter dataframe by selecting values from column Series
#
# Get a column, i.e. dictionary array
last = phys['last']

# Convert a column to pandas series
lastser = phys['last'].to_pandas()
mis.mem_diff    # 36 MiB zero copy

# Convert to numpy array
lastnp = phys['last'].to_pandas().to_numpy()
mis.mem_diff    # 36 MiB zero copy
lastnp2 = phys['last'].to_pandas().to_numpy(dtype='unicode')
mis.mem_diff    # 239 MiB copy

# Save numpy array
np.savez_compressed('/data/test/Parquet/last', arr=lastnp)     # 4.1 MB
lastnpz = np.load('/data/test/Parquet/last.npz', allow_pickle=True)['arr']

# Filtering condition
cond = (lastser == 'WILKINS')
# Get indices
idx = lastser.index[cond]
# Filter Series
lastser.take(idx)
# Filter pandas dataframe
phys.to_pandas().take(idx)

# Alternative method with indices from numpy array
# Filtering condition
cond = (lastnp == 'WILKINS')
# Get indices
idx = np.nonzero(cond)[0]
# Filter pandas dataframe
phys.to_pandas().take(idx)

# Filter Dictionary Array with a boolean mask
last.chunk(0).indices
last.chunk(0).dictionary
last.chunk(0).filter(pa.array(cond))

# pyarray record batches
physbatches = phys.to_batches()[0]  # zero copy
mis.mem_diff    # 2.8 MiB
last_nparr = np.array(physbatches.column(3).to_pylist(), dtype='unicode', copy=False)
mis.mem_diff    # 245 MiB copy
last_nparr.nbytes
last_nparr.size
