"""
More complex version of ``active_storage_simple.py``
with some masks and Dask stuffs.

Minimal code that illustrates the active storage principle in API.
"""
import os
import dask.array as da
from S3netCDF4._s3netCDF4 import s3Dataset as Dataset


# file specifier
root = "/home/valeriu/climate_data/cmip5/output1"
ncfile = "NSF-DOE-NCAR/CESM1-CAM5/historical/mon/atmos/Amon/r1i1p1/v20130313/tas_Amon_CESM1-CAM5_historical_r1i1p1_185001-200512.nc"
filepath = os.path.join(root, ncfile)
maskpath = "some_masky_mask"  # same grid as data in filepath


def load_storage_data(filepath, var):
    test_dataset = Dataset(filepath, "r")
    my_data = test_dataset.variables[var][:]

    return my_data


def get_local_operation(filepath, var, operation, mask=False):
    """
    Simulate computations INSIDE active storage.

    This is a toy and doesn't necessarily refelect
    what's the hap there, inside the Black Box.
    """
    data = load_storage_data(filepath, var)
    if mask:
        if isinstance(mask, str):  # eg areacello
            mask = load_storage_data(maskpath, mask)  # mask IS the var
        elif isinstance(mask, S3netCDF4._s3netCDF4.s3Dataset):
            mask = mask

    chunks = data.chunks
    if operation == "max":
        op_result = [
            data[i, :, :].max() - mask[i, :, :]
            for i in range(sum(chunks[0]))
        ]

    return op_result


def datastore(filepath, var, internal_mask=False, external_mask=None):
    """
    Data payload loader.
    """
    data = load_storage_data(filepath, var)
    mask = None
    # internally stored mask eg fx variables: stflf, areacella, volcello etc
    if internal_mask:
        mask = internal_mask
    if external_mask is not None:
        # careful with grids, datatypes, metadata etc
        # when dealing with external masks
        mask = Dataset(user_mask_path, "r")

    return data, mask


def load(filepath, var, active=False, operation=None,
         internal_mask=False, external_mask=None):
    """
    Get a max with or without active storage participation.

    Parameters
    ----------
    file path: str, path to ``var`` data file on Active Storage
    var: str, variable that user needs its data
    active: bool, use or not the Active Storage for computations
    operation: str, string of linear operation
    internal_mask: str, mask variable name
    external_mas: str, path to external mask file (client)
    """
    mask = None
    if internal_mask:
        mask = internal_mask
    elif external_mask is not None:
        mask = external_mask

    if operation is None:
        if not active:
            data, mask = datastore(filepath, var, internal_mask, external_mask)
            return data, mask
        else:
            return NotImplementedError
    else:
        if not active:
            if mask is not None:
                data, mask = datastore(filepath, var, internal_mask,
                                       external_mask)
                return getattr(mask | data, operation)()
            else:
                data, _ = datastore(filepath, var, internal_mask=False,
                                    external_mask=None)
                return getattr(data, operation)()
        else:
            if mask is not None:
                return getattr(da.from_array(get_local_operation(filepath,
                                                                 var,
                                                                 operation,
                                                                 mask)),
                               operation)()
            else:
                return getattr(da.from_array(get_local_operation(filepath,
                                                                 var,
                                                                 operation)),
                               operation)()
    

# run examples
inactive_max = load(filepath, "tas", operation="max")
active_max = load(filepath, "tas", active=True, operation="max")
print(inactive_max.compute(), active_max.compute())
