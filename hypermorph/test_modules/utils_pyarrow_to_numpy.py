"""
HyperMorph Modules Testing: pyarrow to numpy conversion methods
(C) August 2020 By Athanassios I. Hatzis
"""

import pyarrow as pa
import fletcher as fl
import numpy as np
from hypermorph.utils import DSUtils


#
# Python lists
#

# Without nulls
cols = ["red", "white", "yellow", "brown", "brown", "black", "black", "black",
                    "white", "yellow", "blue", "white", "black", "white"]
ints = [4, 3, 0, 5, 5, 9, 9, 9, 3, 0, 6, 3, 9, 3]
flts = [15.5, 7.2, 3.75, 142.88, 142.88, 90.5, 90.5, 90.5, 7.2, 3.75, 2.1, 7.2, 90.5, 7.2]

# With nulls
col_nulls = ["red", None, "yellow", "brown", "brown", None, None, None, "white", "yellow", "blue", "white", None, "white"]
int_nulls = [4, None, 0, 5, 5, None, None, None, 3, 0, 6, 3, None, 3]
flt_nulls = [15.5, None, 3.75, 142.88, 142.88, None, None, None, 7.2, 3.75, 2.1, 7.2, None, 7.2]


#
# PyArrow Arrays
#
cols_arr = pa.array(cols)
ints_arr = pa.array(ints, type='uint8')
flts_arr = pa.array(flts, type='float32')

col_nulls_arr = pa.array(col_nulls)
int_nulls_arr = pa.array(int_nulls, type='uint8')
flt_nulls_arr = pa.array(flt_nulls, type='float32')

cols_arr_dict = pa.array(cols).dictionary_encode()
ints_arr_dict = pa.array(ints, type='uint8').dictionary_encode()
flts_arr_dict = pa.array(flts, type='float32').dictionary_encode()

col_nulls_arr_dict = pa.array(col_nulls).dictionary_encode()
int_nulls_arr_dict = pa.array(int_nulls, type='uint8').dictionary_encode()
flt_nulls_arr_dict = pa.array(flt_nulls, type='float32').dictionary_encode()


# Test Fletcher conversion to NumPy
DSUtils.pyarrow_to_numpy(flt_nulls_arr_dict, 'float32')
fl.FletcherContinuousArray(flt_nulls_arr_dict).to_numpy(dtype='float32', copy=False, na_value=np.nan)
