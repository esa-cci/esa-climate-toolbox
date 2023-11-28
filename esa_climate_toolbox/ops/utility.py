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
from esa_climate_toolbox.core.types import DatasetLike
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
