from storagecompute import active_open_file


root = "/home/valeriu/climate_data/cmip5/output1"
ncfile = "NSF-DOE-NCAR/CESM1-CAM5/historical/mon/atmos/Amon/r1i1p1/v20130313/tas_Amon_CESM1-CAM5_historical_r1i1p1_185001-200512.nc"

file_parameters = [root, ncfile]

storage_max = active_open_file(file_parameters,
                               operation="max",
                               storage=True)
x = storage_max + 22.

print("Final result (before calling compute): ", x)
print("Final result (after calling compute): ", x.compute())
