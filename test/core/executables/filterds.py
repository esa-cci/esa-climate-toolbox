import sys

import xarray as xr

args = sys.argv
if len(args) < 1 + 3:
    print("usage: filterds.py <in_file> <out_file> <var> ...", file=sys.stderr)
    exit(1)

in_file = args[1]
out_file = args[2]
vars = args[3:]

ds = xr.open_dataset(in_file)
ds = ds.drop_vars(vars)
ds.to_netcdf(out_file)
