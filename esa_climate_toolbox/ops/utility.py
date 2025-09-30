# The MIT License (MIT)
# Copyright (c) 2023 ESA Climate Change Initiative
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
This module provides general utility operations that wrap specific
``xarray`` functions.

The intention is to make available the ``xarray`` API as a set of general,
domain-independent utility functions.

All operations in this module are tagged with the ``"utility"`` tag.

"""
import xarray as xr

from esa_climate_toolbox.core.op import op
from esa_climate_toolbox.core.op import op_input
from esa_climate_toolbox.core.op import op_return
from esa_climate_toolbox.core.types import DatasetLike, VarNamesLike, DimName
from esa_climate_toolbox.core.types import ValidationError


@op(tags=['utility'])
@op_input('ds_1', data_type=DatasetLike)
@op_input('ds_2', data_type=DatasetLike)
@op_input('ds_3', data_type=DatasetLike)
@op_input('ds_4', data_type=DatasetLike)
@op_input('join', value_set=["outer", "inner", "left", "right", "exact"])
@op_input('compat',
          value_set=["identical", "equals", "broadcast_equals", "no_conflicts"]
          )
def merge(ds_1: DatasetLike.TYPE,
          ds_2: DatasetLike.TYPE,
          ds_3: DatasetLike.TYPE = None,
          ds_4: DatasetLike.TYPE = None,
          join: str = 'outer',
          compat: str = 'no_conflicts') -> xr.Dataset:
    """
    Merge up to four datasets to produce a new dataset with combined variables
    from each input dataset.

    This is a wrapper for the ``xarray.merge()`` function.

    For documentation refer to xarray documentation at
    http://xarray.pydata.org/en/stable/generated/xarray.Dataset.merge.html#xarray.Dataset.merge

    The *compat* argument indicates how to compare variables of the same name
    for potential conflicts:

    * "broadcast_equals": all values must be equal when variables are broadcast
      against each other to ensure common dimensions.
    * "equals": all values and dimensions must be the same.
    * "identical": all values, dimensions and attributes must be the same.
    * "no_conflicts": only values which are not null in both datasets must be
        equal. The returned dataset then contains the combination of all
        non-null values.

    :param ds_1: The first input dataset.
    :param ds_2: The second input dataset.
    :param ds_3: An optional 3rd input dataset.
    :param ds_4: An optional 4th input dataset.
    :param join: How to combine objects with different indexes.
    :param compat: How to compare variables of the same name for potential
        conflicts.
    :return: A new dataset with combined variables from each input dataset.
    """

    ds_1 = DatasetLike.convert(ds_1)
    ds_2 = DatasetLike.convert(ds_2)
    ds_3 = DatasetLike.convert(ds_3)
    ds_4 = DatasetLike.convert(ds_4)

    datasets = []
    for ds in (ds_1, ds_2, ds_3, ds_4):
        if ds is not None:
            included = False
            for ds2 in datasets:
                if ds is ds2:
                    included = True
            if not included:
                datasets.append(ds)

    if len(datasets) == 0:
        raise ValidationError('At least two different datasets must be given')
    elif len(datasets) == 1:
        return datasets[0]
    else:
        return xr.merge(datasets, compat=compat, join=join)


@op(tags=["utility", "normalise"])
@op_input("ds", data_type=DatasetLike)
@op_input("var_names", data_type=VarNamesLike)
@op_input("min_value", data_type=float, default_value=0.0)
@op_input("max_value", data_type=float, default_value=1.0)
@op_input("dim", data_type=DimName)
@op_input("suffix", data_type=str)
@op_input("drop_original", data_type=bool, default_value=False)
@op_return(add_history=True)
def normalise_vars(
        ds: DatasetLike.TYPE,
        var_names: VarNamesLike.TYPE = None,
        min_value: float = 0.0,
        max_value: float = 0.0,
        dim: str = "time",
        suffix=None,
        drop_original=False
) -> xr.Dataset:
    """
    Normalises variables of a dataset to a data range.

    :param ds: The dataset containing the variables to be normalised.
    :param var_names: The names of the variables to be normalised.
        If none are given, all variables will be normalised.
        Default is none.
    :param min_value: The lower border of the target value range. Default is 0.0
    :param max_value: The upper border of the target value range. Default is 1.0
    :param dim: Dimension over which to normalise.
        Default is "time"
    :param suffix: Suffix to be appended to the standardised var.
        If no suffix is specified, the original var name will be used
        and the existing variables will not be included in the output.
        Default is None
    :param drop_original: If true and a suffix is given, existing variables
        will be removed.
        Default is True

    :return: A new dataset with normalised variables.
    """
    if min >= max:
        raise ValueError("Parameter 'min' must not be larger than parameter 'max'.")
    var_names = var_names or list(ds.data_vars.keys())
    for var_name in var_names:
        new_var = ((ds[var_name] - ds[var_name].min(dim=dim)) *
                   (max_value - min_value) / (ds[var_name] - ds[var_name].max(dim=dim)))
        new_var_name = f"{var_name}_{suffix}" if suffix is not None else var_name
        ds = ds.assign({new_var_name: new_var})
    if drop_original and suffix is not None:
        ds = ds.drop_vars(var_names)
    return ds


@op(tags=["utility", "standardise"])
@op_input("ds", data_type=DatasetLike)
@op_input("var_names", data_type=VarNamesLike)
@op_input("dim", data_type=DimName)
@op_input("suffix", data_type=str)
@op_input("drop_original", data_type=bool, default_value=False)
@op_return(add_history=True)
def standardise_vars(
        ds: DatasetLike.TYPE,
        var_names: VarNamesLike.TYPE = None,
        dim="time",
        suffix=None,
        drop_original=False
) -> xr.Dataset:
    """
    Standardises variables of a dataset to a scale where
    0 is the variable's mean and 1 is its standard deviation.

    :param ds: The dataset containing the variables to be standardised.
    :param var_names: The names of the variables to be standardised.
        If none are given, all variables will be standardised.
        Default is none.
    :param dim: Dimension over which to build the mean and the
        standard deviation.
        Default is "time"
    :param suffix: Suffix to be appended to the standardised var.
        If no suffix is specified, the original var name will be used
        and the existing variables will not be included in the output.
        Default is None
    :param drop_original: If true and a suffix is given, existing variables
        will be removed.
        Default is True

    :return: A new dataset with standardised variables.
    """
    var_names = var_names or list(ds.data_vars.keys())
    for var_name in var_names:
        new_var = ds[var_name] - (ds[var_name].mean(dim=dim) / ds[var_name].std(dim=dim))
        new_var_name = f"{var_name}_{suffix}" if suffix is not None else var_name
        ds = ds.assign({new_var_name: new_var})
    if drop_original and suffix is not None:
        ds = ds.drop_vars(var_names)
    return ds
