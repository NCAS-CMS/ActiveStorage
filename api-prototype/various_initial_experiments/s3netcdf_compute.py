import os

import numpy as np
import S3netCDF4
from dask.distributed import Client
from S3netCDF4._s3netCDF4 import s3Dataset as Dataset


def load_storage_data(root, ncfile):
    ncfile = os.path.join(root, ncfile)
    test_dataset = Dataset(ncfile, "r")
    my_data = test_dataset.variables['tas'][:]

    # chunk data
    chunked_data = np.array_split(my_data, 3)  # 3 equal chunks
    return chunked_data


def storage_compute(file_parameters, operation):
    # load chunked data into the cluster
    root, ncfile = file_parameters
    chunked_data = load_storage_data(root, ncfile)

    # run computation
    if operation == "max":
        result = np.array([np.max(chunk) for chunk in chunked_data])

    return result


if __name__ == "__main__":
    client = Client()
    storage_compute(root, ncfile, operation)
