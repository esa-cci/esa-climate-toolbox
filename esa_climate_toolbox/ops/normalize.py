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
Description
===========

Dataset normalization operation.

Components
==========
"""

import xarray as xr

from esa_climate_toolbox.core.op import op
from esa_climate_toolbox.core.op import op_return
from esa_climate_toolbox.core.opimpl import adjust_temporal_attrs_impl
import xcube.core.normalize as xcube_normalize


@op(tags=['utility'], version='1.0')
@op_return(add_history=True)
def normalize(ds: xr.Dataset) -> xr.Dataset:
    """
    Normalize the geo- and time-coding upon opening the given dataset w.r.t.
    to a common (CF-compatible) convention used within the ESA Climate Toolbox.
    This will maximize the compatibility of a dataset for usage with operations.

    That is,
    * variables named "latitude" will be renamed to "lat";
    * variables named "longitude" or "long" will be renamed to "lon";

    Then, for equi-rectangular grids,
    * Remove 2D "lat" and "lon" variables;
    * Two new 1D coordinate variables "lat" and "lon" will be generated from
    original 2D forms.

    Finally, it will be ensured that a "time" coordinate variable will be of
    type *datetime*.

    :param ds: The dataset to normalize.
    :return: The normalized dataset, or the original dataset,
        if it is already "normal".
    """
    return xcube_normalize.normalize_dataset(ds, reverse_decreasing_lat=True)


@op(tags=['utility'], version='1.0')
def adjust_spatial_attrs(
        ds: xr.Dataset, allow_point: bool = False
) -> xr.Dataset:
    """
    Adjust the global spatial attributes of the dataset by doing some
    introspection of the dataset and adjusting the appropriate attributes
    accordingly.

    In case the determined attributes do not exist in the dataset, these will
    be added.

    For more information on suggested global attributes see
    `Attribute Convention for Data Discovery
    <http://wiki.esipfed.org/index.php/Attribute_Convention_for_Data_Discovery>`_

    :param ds: Dataset to adjust
    :param allow_point: Whether a dataset containing a single point is allowed
    :return: Adjusted dataset
    """
    return xcube_normalize.adjust_spatial_attrs(ds, allow_point=allow_point)


@op(tags=['utility'], version='1.0')
def adjust_temporal_attrs(ds: xr.Dataset) -> xr.Dataset:
    """
    Adjust the global temporal attributes of the dataset by doing some
    introspection of the dataset and adjusting the appropriate attributes
    accordingly.

    In case the determined attributes do not exist in the dataset, these will
    be added.

    If the temporal attributes exist, but the dataset lacks a variable 'time', a
    new dimension 'time' of size one will be added and related coordinate
    variables 'time' and 'time_bnds' are added to the dataset. The dimension of
    all non-coordinate variables will be expanded by the new time dimension.

    For more information on suggested global attributes see
    `Attribute Convention for Data Discovery
    <http://wiki.esipfed.org/index.php/Attribute_Convention_for_Data_Discovery>`_

    :param ds: Dataset to adjust
    :return: Adjusted dataset
    """
    return adjust_temporal_attrs_impl(ds)
