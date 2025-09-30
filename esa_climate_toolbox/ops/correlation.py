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

"""
Description
===========

Provides methods for determining correlations between variables.

Components
==========
"""

import dask.array as da
import numpy as np
from sklearn.cross_decomposition import CCA
from typing import List
import xarray as xr

from xcube.core.gridmapping import GridMapping

from esa_climate_toolbox.core.op import op
from esa_climate_toolbox.core.op import op_input
from esa_climate_toolbox.core.op import op_return
from esa_climate_toolbox.core.types import DatasetLike, VarNamesLike, DimName, ValidationError, DimNamesLike

MAX_CHUNK_SIZE = 2560 * 2560


@op(tags=['correlation', "multivariate"], version='1.0')
@op_input('ds_1', data_type=DatasetLike)
@op_input('ds_1_var_names', data_type=VarNamesLike)
@op_input('ds_2', data_type=DatasetLike)
@op_input('ds_2_var_names', data_type=VarNamesLike)
@op_input('dim', data_type=DimName)
@op_return(add_history=True)
def pairwise_var_correlation(
        ds_1: DatasetLike.TYPE,
        ds_2: DatasetLike.TYPE,
        ds_1_var_names: VarNamesLike.TYPE=None,
        ds_2_var_names: VarNamesLike.TYPE=None,
        dim: str = "time"
) -> xr.Dataset:
    """
    Computes correlations between each pair of variables of two datasets.
    Note that the datasets are expected to have the same spatial and temporal
    grids. consider applying 'coregister' before using this operation.

    :param ds_1: The first dataset.
    :param ds_2: The second dataset.
    :param ds_1_var_names: The names of the variables in the first dataset
        that shall be considered.
        If none are given, all variables will be standardised.
        Default is none.
    :param ds_2_var_names: The names of the variables in the second dataset
        that shall be considered.
        If none are given, all variables will be standardised.
        Default is none.
    :param dim: Dimension over which to compute the correlation.
        Default is "time"

    :return: A new dataset with n*m data arrays, where n is the number of
        variables from the first dataset and m is the number of variables from
        the second dataset. Each data array's name will indicate the combination
        of variables.
    """
    ds_1_var_names = ds_1_var_names or list(ds_1.data_vars.keys())
    ds_2_var_names = ds_2_var_names or list(ds_2.data_vars.keys())

    correlations = dict()
    for ds_1_var_name in ds_1_var_names:
        for ds_2_var_name in ds_2_var_names:
            name = f"corr_{ds_1_var_name}_{ds_2_var_name}"
            correlations[name] = xr.corr(ds_1[ds_1_var_name], ds_2[ds_2_var_name], dim=dim)
    return xr.Dataset(correlations)


def _validate_coords(ds_1: xr.Dataset, ds_2: xr.Dataset):
    for coord in ds_1.coords:
        if not coord in ds_2.coords:
            raise ValidationError(
                f"Datasets must have the same set of coordinates. "
                f"'{coord}' was only found in one dataset."
            )
        if not np.array_equal(ds_1[coord], ds_2[coord]):
            raise ValidationError(
                f"Datasets must have the same set of coordinates. "
                f"The datasets have different values for '{coord}'."
            )

    for coord in ds_2.coords:
        if not coord in ds_1.coords:
            raise ValidationError(
                f"Datasets must have the same set of coordinates. "
                f"'{coord}' was only found in one dataset."
            )
        if not np.array_equal(ds_1[coord], ds_2[coord]):
            raise ValidationError(
                f"Datasets must have the same set of coordinates. "
                f"The datasets have different values for '{coord}'."
            )

@op(tags=['correlation', 'multivariate'], version='1.0')
@op_input('ds_1', data_type=DatasetLike)
@op_input('ds_1_var_names', data_type=VarNamesLike)
@op_input('ds_2', data_type=DatasetLike)
@op_input('ds_2_var_names', data_type=VarNamesLike)
@op_input('num_components', data_type=int, default_value=2)
@op_return(add_history=True)
def pixelwise_group_correlation(
        ds_1: DatasetLike.TYPE,
        ds_2: DatasetLike.TYPE,
        ds_1_var_names: VarNamesLike.TYPE=None,
        ds_2_var_names: VarNamesLike.TYPE=None,
        num_components: int=2
) -> xr.Dataset:
    """
    Computes for each pixel the correlation between groups of variables
    of two datasets. This method uses internally a canonical correlation
    analysis (CCA). The datasets are expected to have the same spatial and
    temporal grids. Consider applying 'coregister' before using this operation.

    :param ds_1: The first dataset.
    :param ds_2: The second dataset.
    :param ds_1_var_names: The names of the variables in the first dataset
        that shall be considered.
        If none are given, all variables will be standardised.
        Default is none.
    :param ds_2_var_names: The names of the variables in the second dataset
        that shall be considered.
        If none are given, all variables will be standardised.
        Default is none.
    :param num_components: The number of canonical components to be computed.

    :return: A new dataset with a single data array that shows for each pixel
        the correlation between variables from the first dataset and variables
        from the second dataset.
    """

    _validate_coords(ds_1, ds_2)
    if "time" not in ds_1.dims:
        raise ValidationError(
            "Can perform correlation analysis on datasets with "
            "a dimension labeled 'time'."
        )
    xy_dim_names = list(GridMapping.from_dataset(ds_1).xy_dim_names)

    dims_set = set(xy_dim_names)
    dims_set.add("time")
    diff = set(ds_1.dims) - dims_set
    if len(diff) > 0:
        raise ValidationError(
            "Can perform correlation analysis only on datasets with "
            "temporal and spatial dimensions."
            "Consider building a subset of your data."
        )

    ds_1_var_names = ds_1_var_names or list(ds_1.data_vars.keys())
    ds_2_var_names = ds_2_var_names or list(ds_2.data_vars.keys())

    ds_1_as_array = xr.concat([ds_1[v] for v in ds_1_var_names], dim="var")
    ds_2_as_array = xr.concat([ds_2[v] for v in ds_2_var_names], dim="var")

    if ds_1_as_array.dims[1] != "time" or ds_2_as_array.dims[1] != "time":
        raise ValidationError(
            "Dimensions are not in the correct order. Please consider normalising your datasets."
        )

    larger_var_dim = max(len(ds_1_var_names), len(ds_2_var_names))
    new_chunk_sizes = _determine_new_chunk_sizes(ds_1_as_array, larger_var_dim)
    new_chunk_sizes["var"] = len(ds_1_var_names)
    ds_1_as_array = ds_1_as_array.chunk(new_chunk_sizes)
    new_chunk_sizes["var"] = len(ds_2_var_names)
    ds_2_as_array = ds_2_as_array.chunk(new_chunk_sizes)

    def cca_for_pixel(x, y):
        if da.isnan(x).any() or da.isnan(y).any():
            return np.nan
        cca = CCA(n_components=num_components)
        cca.fit(x, y)
        x_scores, y_scores = cca.transform(x, y)
        return np.corrcoef(x_scores[:,0], y_scores[:,0])[0,1]

    def apply_block(ds_1_block: xr.DataArray, ds_2_block):
        out = da.full(ds_1_block.shape[2:], np.nan)
        for i in range(ds_1_block.shape[2]):
            for j in range(ds_1_block.shape[3]):
                out[i,j] = cca_for_pixel(ds_1_block[:, :, i, j], ds_2_block[:, :, i, j])
        out_dim_1 = ds_1_block.dims[-2]
        out_dim_2 = ds_1_block.dims[-1]
        return xr.DataArray(
            out,
            coords={out_dim_1: ds_1_block[out_dim_1], out_dim_2: ds_1_block[out_dim_2]},
            dims=(out_dim_1, out_dim_2)
        )

    template_sizes = []
    template_chunks = []
    template_coords = {}
    template_dims = []
    for dim in ds_1_as_array.dims:
        if dim not in xy_dim_names:
            continue
        template_sizes.append(ds_1_as_array.sizes[dim])
        template_chunks.append(ds_1_as_array.chunksizes[dim])
        template_coords[dim] = ds_1_as_array[dim]
        template_dims.append(dim)

    template = xr.DataArray(
        da.empty(tuple(template_sizes),
                 chunks=tuple(template_chunks),
                 dtype=float),
        coords=template_coords,
        dims=template_dims
    )

    result = xr.map_blocks(
        apply_block,
        obj=ds_1_as_array,
        args=[ds_2_as_array],
        template=template
    )
    return result.to_dataset(name="canonical_correlation")


def _determine_new_chunk_sizes(
        data_array: xr.DataArray,
        var_dim_size: int
) -> dict:
    chunk_sizes = {k: v[0] for k, v in data_array.chunksizes.items()}
    chunk_sizes["var"] = var_dim_size
    chunk_sizes["time"] = data_array.sizes["time"]
    chunk_size = np.prod(list(chunk_sizes.values()))
    while chunk_size > MAX_CHUNK_SIZE:
        max_chunk_size = 1
        max_chunk_size_key = None
        new_chunk_size = None
        for k, v in chunk_sizes.items():
            if k not in ["var", "time"] and v > max_chunk_size:
                remainder = 1
                divisor = 1
                while remainder != 0 and divisor < 10:
                    divisor += 1
                    remainder = v % divisor
                if remainder != 0:
                    continue
                max_chunk_size = v
                max_chunk_size_key = k
                new_chunk_size = v / divisor
        if max_chunk_size_key is None:
            break
        chunk_sizes[max_chunk_size_key] = int(new_chunk_size)
        chunk_size = np.prod([v for k, v in chunk_sizes.items()])
    return chunk_sizes
