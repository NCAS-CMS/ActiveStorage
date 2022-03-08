import os
import sys

import iris
import numpy as np
from dask import delayed

from s3netcdf_compute import storage_compute


def local_data_loader(file_parameters):
    root, ncfile = file_parameters
    filename = os.path.join(root, ncfile)
    cube = iris.load_cube(filename)
    return cube.core_data()


def get_max(x, y=None, storage=False):
    if storage:
        max_val = np.max(y)  # a reduced data array composed of local maxima computed by storage
    else:
        max_val = np.max(x)  # full data array from locally loading the data
    return max_val


def active_open_file(file_parameters, operation=None, storage=False):
    if operation is None:
        print("No operation supplied to active open;"
              "please select one or use regular open.")
        sys.exit(1)
    if not storage:
        x = local_data_loader(file_parameters)
        if operation == "max":
            return delayed(get_max)(x)
        else:
            return "NotImplementedError"

    x = []
    if operation == "max":
        y = storage_compute(file_parameters, operation=operation)
        return delayed(get_max)(x, y, storage=True)
    else:
        return "NotImplementedError"
