"""
Minimal code that illustrates the active storage principle in API.
"""
import os
from S3netCDF4._s3netCDF4 import s3Dataset as Dataset


# file specifier
root = "/home/valeriu/climate_data/cmip5/output1"
ncfile = "NSF-DOE-NCAR/CESM1-CAM5/historical/mon/atmos/Amon/r1i1p1/v20130313/tas_Amon_CESM1-CAM5_historical_r1i1p1_185001-200512.nc"
filepath = os.path.join(root, ncfile)


def load_storage_data(filepath, var, slices):
    """Load specified data using S3 dataset."""
    if slices == ":":
        return Dataset(filepath, "r").variables[var][:]
    elif isinstance(slices, list):
        s1, s2 = slices
        return Dataset(filepath, "r").variables[var][s1:s2]
    else:
        return ValueError('Slices must be either ":" or a two member list.')


def active_storage_operation(filepath, var, operation, slices):
    """
    Simulate computations INSIDE active storage.

    This is a toy and doesn't necessarily refelect
    what's the hap there, inside the Black Box.
    """
    data = load_storage_data(filepath, var, slices)
    if operation == "max":
        op_result = data.max()
    if operation == "min":
        op_result = data.min()  # and so on ...

    return op_result


def load(filepath, var, slices, active=False, operation=None):
    """
    Get a max with or without active storage participation.

    Parameters
    ----------
    file path: str, path to ``var`` data file on Active Storage
    var: str, variable that user needs its data
    active: bool, use or not the Active Storage for computations
    operation: str, string of linear operation
    """

    if not active:
        if operation is not None:
            return getattr(load_storage_data(filepath, var, slices), operation)()
        else:
            return load_storage_data(filepath, var, slices)
    else:
        if operation is not None:
            return getattr(active_storage_operation(filepath,
                                                    var,
                                                    operation,
                                                    slices),
                           operation)()
        else:
            return NotImplementedError("To use Active Storage, "
                                       "supply an operation first!")
    

# run examples
simple_load = load(filepath, "tas", slices=":")
inactive_max = load(filepath, "tas", slices=":", operation="max")
active_max = load(filepath, "tas", slices="[1990, 2014]",
                  active=True, operation="max")
