# The MIT License (MIT)
# Copyright (c) 2025 ESA Climate Change Initiative
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import xarray as xr

from xcube.core.gridmapping import GridMapping

from typing import List
from typing import Union

def output_array_as_image(array: xr.DataArray, filepath: str = None, out_format: str = "png") -> None:
    """
    Writes out a 2-dimensional data array as either a png or a geotiff.

    :param array:       The array to be written.
    :param filepath:    The path to which the output file shall be written.
    :param out_format:  The format of the output file. Must be either 'png' or 'gtiff'
    """
    if len(array.dims) != 2:
        array_dims = ", ".join(array.dims)
        raise ValueError(f"Function is only available for 2-dimensional data arrays,"
                         f"array has dimensions '{array_dims}'")
    if out_format not in ["png", "gtiff"]:
        raise ValueError(f"Unsupported output format. Only support 'png' and 'gtiff', "
                         f"found '{out_format}'.")
    if filepath is not None:
        if not filepath.endswith(out_format):
            filepath = f"{filepath}.{out_format}"
    else:
        filepath = f"{array.name}.{out_format}"
    if out_format == "png":
        plt.imsave(filepath, np.nan_to_num(array.values), cmap='bone')
    elif out_format == "gtiff":
        array.rio.to_raster(filepath, driver="GTiff")

def output_dataset_as_geotiff(
        ds: xr.Dataset,
        var_names: List[str] = None,
        timestamp: Union[str, pd.Timestamp] = None,
        timeindex: int = -1,
        filepath: str = None
) -> str:
    """
    Writes out a dataset as 3-dimensional geotiff, where the third dimension "band"
    comprises the data arrays.

    :param dataset:     The dataset to be written. If it is not already 2-dimensional,
        either parameter timestamp or timeindex can be set to determine a timestep.
    :param var_names:   The names of the variables to be written to the geotiff.
        If not given, all appropriate variables of the dataset will be written out.
    :param timestamp:   A string or pandas timestamp to determine a dataset timestep
        to be written. Must not be used together with timeindex.
    :param timeindex:   An index of the time dimension to determine a dataset timestep
        to be written. Must not be used together with timestamp.
    :param filepath:    The path to which the output file shall be written.
    """
    if timeindex >= 0:
        if timestamp is not None:
            raise ValueError("Only one of parameters timestamp and timeindex may be set.")
        ds = ds.isel(time=timeindex)
    elif timestamp is not None:
        if isinstance(timestamp, pd.Timestamp):
            timestamp = timestamp.strftime("%Y-%m-%dT%H:%M:%S")
        ds = ds.sel(time=timestamp, method="nearest")
    ds = ds.squeeze()
    gm = GridMapping.from_dataset(ds)
    if var_names is not None:
        for var_name in var_names:
            if set(gm.xy_dim_names) != set(ds[var_name].dims):
                expected = ", ".join(gm.xy_dim_names)
                actual = ", ".join(ds[var_name].dims)
                raise ValueError(f"Variable '{var_name}' was supposed to have dimensions ({expected}), "
                                 f"found ({actual})")
    else:
        var_names = []
        for var in ds.data_vars:
            if set(gm.xy_dim_names) == set(ds[var].dims):
                var_names.append(var)
    if len(var_names) == 0:
        raise ValueError("No appropriate variables found in dataset.")
    ds = ds[var_names]
    if filepath is not None:
        if not filepath.endswith(".tif") and not filepath.endswith(".gtiff"):
            filepath = f"{filepath}.tif"
    else:
        filepath = f"{ds.attrs.get("title", "output")}.tif"
    array = ds.to_array(dim="band")
    array.rio.to_raster(filepath, driver="GTiff")
    return filepath

def output_animation(array: xr.DataArray, output_folder: str = "animation", img_format: str = "png"):
    """
    Writes out a data array with one temporal and two spatial dimensions as a sequence of
    image files.

    :param array:           The array to be written.
    :param output_folder:   The folder to which the output files shall be written.
    :param img_format:      The format in which the output files shall be written.
        Must be either "png" or "gtiff", default is "png"
    """
    if len(array.dims) != 3:
        array_dims = ", ".join(array.dims)
        raise ValueError(f"Function is only available for 3-dimensional data arrays,"
                         f"array has dimensions '{array_dims}'")
    if img_format not in ["png", "gtiff"]:
        raise ValueError(f"Unsupported output format. Only support 'png' and 'gtiff', "
                         f"found '{img_format}'.")
    os.makedirs(output_folder, exist_ok=True)
    for i in range(len(array.time)):
        timestamp = pd.Timestamp(array.time.values[i]).strftime("%Y-%m-%dT%H:%M:%S")
        sub_array = array.isel(time=i).squeeze()
        output_array_as_image(sub_array, f"{output_folder}/{timestamp}.{img_format}", out_format=img_format)
