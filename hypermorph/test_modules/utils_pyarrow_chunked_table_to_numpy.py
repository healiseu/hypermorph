import pyarrow as pa
import numpy as np
from hypermorph.utils import DSUtils as dsu

#
# PyArrow Types
#
t1 = pa.uint16()
t2 = pa.utf8()
t3 = pa.float32()
t4 = pa.bool_()

column_names = ['pid', 'name', 'age', 'color', 'height', 'alive']

#
# PyArrow Table from PyArrow arrays and schema
#
pid = pa.array(['p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7', 'p8', 'p9'], type=t2)
name = pa.array(['Tom', 'John', 'Ann', 'Mary', 'Ruth', 'Ian', 'John', 'Tom', 'Ann'], type=t2)
age = pa.array([70, 90, 10, 70, 85, 89, 20, 10, 70], type=t1)
color = pa.array(['red', 'black', 'white', 'red', 'black', 'black', 'yellow', 'white', 'red'], type=t2)
height = pa.array([1.85, 1.72, 1.02, 1.82, 1.62, 1.53, 1.67, 1.59, 1.79], type=t3)
alive = pa.array([True, False, True, False, False, False, True, True, True], type=t4)

batch = pa.RecordBatch.from_arrays([pid, name, age, color, height, alive], names=column_names)

tbl = pa.Table.from_batches([batch]*3)

#
# PyArrow Table with dictionary fields from PyArrow arrays
#
batch_dict = pa.RecordBatch.from_arrays([pid.dictionary_encode(), name.dictionary_encode(),
                                         age.dictionary_encode(), color.dictionary_encode(),
                                         height.dictionary_encode(), alive.dictionary_encode()],
                                        names=['pid', 'name', 'age', 'color', 'height', 'alive'])
tbl_dict = pa.Table.from_batches([batch_dict]*3)


name_na = pa.array([None, None, 'Ann', 'Mary', None, 'Ian', 'John', 'Tom', 'Ann'], type=t2)
age_na = pa.array([70, None, 10, None, None, 89, 20, 10, 70], type=t1)
color_na = pa.array(['red', None, 'white', 'red', 'black', 'black', 'yellow', None, None], type=t2)
height_na = pa.array([1.85, 1.72, 1.02, 1.82, None, 1.53, None, None, 1.79], type=t3)
alive_na = pa.array([True, None, None, False, None, False, True, None, True], type=t4)

batch_dict_na= pa.RecordBatch.from_arrays([pid.dictionary_encode(), name_na.dictionary_encode(),
                                    age_na.dictionary_encode(), color_na.dictionary_encode(),
                                    height_na.dictionary_encode(), alive_na.dictionary_encode()],
                                    names=['pid', 'name', 'age', 'color', 'height', 'alive'])
tbl_dict_na = pa.Table.from_batches([batch_dict_na]*3)


# Conversion from Chunked Arrays back to Dict Arrays
pid_dict = dsu.pyarrow_chunked_to_dict(tbl_dict['pid'])
name_dict = dsu.pyarrow_chunked_to_dict(tbl_dict['name'])
color_dict = dsu.pyarrow_chunked_to_dict(tbl_dict['color'])
age_dict = dsu.pyarrow_chunked_to_dict(tbl_dict['age'])
height_dict = dsu.pyarrow_chunked_to_dict(tbl_dict['height'])
alive_dict = dsu.pyarrow_chunked_to_dict(tbl_dict['alive'])

name_na_dict = dsu.pyarrow_chunked_to_dict(tbl_dict_na['name'])
color_na_dict = dsu.pyarrow_chunked_to_dict(tbl_dict_na['color'])
age_na_dict = dsu.pyarrow_chunked_to_dict(tbl_dict_na['age'])
height_na_dict = dsu.pyarrow_chunked_to_dict(tbl_dict_na['height'])
alive_na_dict = dsu.pyarrow_chunked_to_dict(tbl_dict_na['alive'])


# String conversion
dsu.pyarrow_to_numpy(pid_dict)
dsu.pyarrow_to_numpy(name_dict)
dsu.pyarrow_to_numpy(color_dict)

dsu.pyarrow_to_numpy(name_na_dict)
dsu.pyarrow_to_numpy(color_na_dict)

# Integer conversion
dsu.pyarrow_to_numpy(age_dict)
dsu.pyarrow_to_numpy(age_na_dict)

# Float conversion
dsu.pyarrow_to_numpy(height_dict)
dsu.pyarrow_to_numpy(height_na_dict)

# Boolean conversion
dsu.pyarrow_to_numpy(alive_dict)
dsu.pyarrow_to_numpy(alive_na_dict)
