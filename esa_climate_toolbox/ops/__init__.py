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

The ``ops`` package comprises all specific operation and processor
implementations.

This is a plugin package automatically imported by the installation script's
entry point ``esa_climate_toolbox_ops`` see the projects ``setup.py`` file).

Verification
============

The module's unit-tests are located in
`test/ops <https://github.com/esa-cci/esa-climate-toolbox/tree/main/test/ops>`_
and may be executed using
``$ py.test test/ops/test_<MODULE>.py --cov=esa-climate-toolbox/ops/<MODULE>.py``
for extra code coverage information.

Functions
==========
"""


# We need ect_init being accessible to use by the plugin registering logic
# before any attempt to import any of the submodules is made.


def ect_init():
    # Plugin initializer.
    # Left empty because operations are registered automatically via decorators.
    pass


from .coregistration import coregister
from .normalize import adjust_spatial_attrs
from .normalize import adjust_temporal_attrs
from .normalize import normalize
from .resampling import downsample_2d
from .resampling import resample_2d
from .resampling import upsample_2d
from .subset import subset_spatial
from .subset import subset_temporal
from .subset import subset_temporal_index
from .timeseries import tseries_mean
from .timeseries import tseries_point
from.utility import merge

__all__ = [
    # .coregistration
    'coregister',
    # .normalize
    'normalize',
    'adjust_temporal_attrs',
    'adjust_spatial_attrs',
    # .resampling
    'resample_2d',
    'downsample_2d',
    'upsample_2d',
    # .subset
    'subset_spatial',
    'subset_temporal',
    'subset_temporal_index',
    # .timeseries
    'tseries_point',
    'tseries_mean',
    # .utility
    'merge'
]
