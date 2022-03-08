"""
Minimal code that illustrates the active storage principle in API.
"""
import numpy as np
import netCDF4
from netCDF4 import Dataset



def active_storage(var, slices, operation=None):
    """Mock a dataset with strides and slices."""
    # mock a netCDF4 dataset
    dataset = Dataset()
    dataset.createVariable(var, np.float32,
                           dimensions=('time', 'lat', 'lon'))

    # mock two strides (in time); note the fill_value
    dataset.variables[var][0] = np.full((2, 2), fill_value=1.e-20,
                                        dtype=np.float32)
    dataset.variables[var][1] = np.full((2, 2), fill_value=1.e-20,
                                        dtype=np.float32)
    # slice if needed; simple approach: all slices
    # or assume s1 in stride1 and s2 in stride2
    if slices == ":":
        if operation is not None:
            if operation == "max":
                return [dataset.variables[var][0].max(),
                        dataset.variables[var][1].max()]
        else:
            return netCDF4.MFDataset([dataset.variables[var][0],
                                      dataset.variables[var][1]])
    elif isinstance(slices, list):
        s1, s2 = slices
        if operation is not None:
            if operation == "max":
                return [dataset.variables[var][0][s1:].max(),
                        dataset.variables[var][1][:s2].max()]
        else:
            return netCDF4.MFDataset([dataset.variables[var][0][s1:],
                                      dataset.variables[var][1][:s2]])
    else:
        return ValueError('Slices must be either ":" or a two member list.')


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
            return getattr(active_storage(var, slices), operation)()
        else:
            return active_storage(var, slices)
    else:
        if operation is not None:
            return getattr(active_storage(var,
                                          slices,
                                          operation),
                           operation)()
        else:
            return NotImplementedError("To use Active Storage, "
                                       "supply an operation first!")
    

# run examples
simple_load = load(filepath, "tas", slices=":")
inactive_max = load(filepath, "tas", slices=":", operation="max")
active_max = load(filepath, "tas", slices="[1990, 2014]",
                  active=True, operation="max")
