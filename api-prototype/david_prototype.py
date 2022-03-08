import numpy as np
from dask import array as da
from dask.highlevelgraph import HighLevelGraph
from dask.array.core import Array
from dask.array.utils import meta_from_array
import netCDF4
from pprint import pprint

class NetCDFArray:
    """An underlying array stored in a netCDF file.

    .. versionadded:: (cfdm) 1.7.0

    """

    def __init__(
        self,
        filename=None,
        ncvar=None,
        varid=None,
        group=None,
        dtype=None,
        ndim=None,
        shape=None,
        size=None,
        mask=True,
    ):
        """**Initialisation**

        :Parameters:

            filename: `str`
                The name of the netCDF file containing the array.

            ncvar: `str`, optional
                The name of the netCDF variable containing the
                array. Required unless *varid* is set.

            group: `None` or sequence of `str`, optional
                Specify the netCDF4 group to which the netCDF variable
                belongs. By default, or if *group* is `None` or an
                empty sequence, it assumed to be in the root
                group. The last element in the sequence is the name of
                the group in which the variable lies, with other
                elements naming any parent groups (excluding the root
                group).

                *Parameter example:*
                  To specify that a variable is in the root group:
                  ``group=()`` or ``group=None``

                *Parameter example:*
                  To specify that a variable is in the group '/forecasts':
                  ``group=['forecasts']``

                *Parameter example:*
                  To specify that a variable is in the group
                  '/forecasts/model2': ``group=['forecasts', 'model2']``

                .. versionadded:: (cfdm) 1.8.6.0

            dtype: `numpy.dtype`
                The data type of the array in the netCDF file. May be
                `None` if the numpy data-type is not known (which can be
                the case for netCDF string types, for example).

            shape: `tuple`
                The array dimension sizes in the netCDF file.

            size: `int`
                Number of elements in the array in the netCDF file.

            ndim: `int`
                The number of array dimensions in the netCDF file.

            mask: `bool`
                If False then do not mask by convention when reading data
                from disk. By default data is masked by convention.

                A netCDF array is masked depending on the values of any of
                the netCDF variable attributes ``valid_min``,
                ``valid_max``, ``valid_range``, ``_FillValue`` and
                ``missing_value``.

                .. versionadded:: (cfdm) 1.8.2

        **Examples:**

        >>> import netCDF4
        >>> nc = netCDF4.Dataset('file.nc', 'r')
        >>> v = nc.variable['tas']
        >>> a = NetCDFFileArray(filename='file.nc', ncvar='tas',
        ...                     group=['forecast'], dtype=v.dtype,
        ...                     ndim=v.ndim, shape=v.shape, size=v.size)

        """
        self.filename = filename
        self.ncvar = ncvar
        self.group = group
        self.dtype = dtype
        self.ndim = ndim
        self.shape = shape
        self.size = size
        self.mask = mask

    def __getitem__(self, indices):
        """Returns a subspace of the array as a numpy array.

        x.__getitem__(indices) <==> x[indices]

        The indices that define the subspace must be either `Ellipsis` or
        a sequence that contains an index for each dimension. In the
        latter case, each dimension's index must either be a `slice`
        object or a sequence of two or more integers.

        Indexing is similar to numpy indexing. The only difference to
        numpy indexing (given the restrictions on the type of indices
        allowed) is:

          * When two or more dimension's indices are sequences of integers
            then these indices work independently along each dimension
            (similar to the way vector subscripts work in Fortran).

        .. versionadded:: (cfdm) 1.7.0

        """
        netcdf = self.open()

        # Traverse the group structure, if there is one (CF>=1.8).
        group = self.group
        if group:
            for g in group[:-1]:
                netcdf = netcdf.groups[g]

            netcdf = netcdf.groups[group[-1]]

        # Get the variable by netCDF name
        variable = netcdf.variables[self.ncvar]
        
        variable.set_auto_mask(self.mask)

        # Read the data
        array = variable[indices]

        # Close the netCDF file
        self.close()

        mx = np.max(array, keepdims=True)

        print("maximum 'coming from storage':", mx)
        return mx

    def __repr__(self):
        """Returns a printable representation of the `NetCDFArray`.

        x.__repr__() is logically equivalent to repr(x)

        """
        return str(self)

    def __str__(self):
        """Returns a string version of the `NetCDFArray` object.

        x.__str__() is logically equivalent to str(x)

        """
        return (
            f"<{self.__class__.__name__}{self.shape}: "
            f"file={self.filename} variable={self.ncvar}>"
        )

    def close(self):
        """Close the `netCDF4.Dataset` for the file containing the data.

        .. versionadded:: (cfdm) 1.7.0

        :Returns:

            `None`

        **Examples:**

        >>> a.close()

        """
        netcdf = getattr(self, "netcdf", None)
        if netcdf is None:
            return

        del self.netcdf

    def open(self):
        """Returns an open `netCDF4.Dataset` for the array's file.

        .. versionadded:: (cfdm) 1.7.0

        :Returns:

            `netCDF4.Dataset`

        **Examples:**

        >>> netcdf = a.open()
        >>> variable = netcdf.variables[a.ncvar()]
        >>> variable.getncattr('standard_name')
        'eastward_wind'

        """
        netcdf = getattr(self, "netcdf", None)
        if netcdf is None:
            try:
                netcdf = netCDF4.Dataset(self.filename, "r")
            except RuntimeError as error:
                raise RuntimeError(f"{error}: {self.filename}")

            self.netcdf = netcdf

        return netcdf


if __name__ == "__main__":

    # Create object for the whole netCDF variable data that has the
    # required API (i.e. numpy-like ndim, shape, size, dtype,
    # __getitem__)
    nc = NetCDFArray(
        filename="test1.nc",
        ncvar="tas",
        dtype=np.dtype("float32"),
        ndim=3,
        shape=(12, 64, 128),
        size=98304,
    )

    for i, chunks in enumerate(
        (
            "auto",
            (6, -1, -1),
            (12, 16, 100),
        )
    ):
        # Create a chunked dask view of the netCDF variable data
        d = da.from_array(
            nc,
            asarray=False,
            # Concurrent reads are supported because __getitem__
            # opens its own independent netCDF4.Dataset instance
            lock=False,
            chunks=chunks,
        )

        print("\nChunks:", d.chunks)

        # Apply the maximum operator lazily
        mx = d.max()
      
        mx.visualize(filename=f"v{i}.png")

        # Compute the lazy operations (read, maximum)
        print("Mean =", mx.compute())
        print("--------------------")

