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


from .aggregate import climatology
from .aggregate import reduce
from .aggregate import statistics
from .aggregate import temporal_aggregation
from .animate import animate_map
from .anomaly import anomaly_external
from .anomaly import anomaly_internal
from .arithmetics import arithmetics
from .arithmetics import diff
from .coregistration import coregister
from .correlation import pairwise_var_correlation
from .correlation import pixelwise_group_correlation
from .data_frame import add_dataset_values_to_geodataframe
from .data_frame import aggregate_statistics
from .data_frame import as_geodataframe
from .data_frame import data_frame_max
from .data_frame import data_frame_min
from .data_frame import data_frame_subset
from .data_frame import find_closest
from .data_frame import query
from .data_frame import to_dataframe
from .data_frame import to_dataset
from .gapfilling import gapfill
from .normalize import adjust_spatial_attrs
from .normalize import adjust_temporal_attrs
from .normalize import normalize
from .outliers import detect_outliers
from .plot import plot
from .plot import plot_contour
from .plot import plot_hist
from .plot import plot_line
from .plot import plot_map
from .plot import plot_scatter
from .resampling import downsample_2d
from .resampling import resample
from .resampling import resample_2d
from .resampling import upsample_2d
from .select import select_features
from .select import select_var
from .subset import subset_spatial
from .subset import subset_temporal
from .subset import subset_temporal_index
from .timeseries import fourier_analysis
from .timeseries import tseries_mean
from .timeseries import tseries_point
from .utility import merge
from .utility import normalise_vars
from .utility import standardise_vars

__all__ = [
    # aggregation
    'climatology',
    'reduce',
    'statistics',
    'temporal_aggregation',
    # animation
    'animate_map',
    # anomaly
    'anomaly_external',
    'anomaly_internal',
    # arithmetics
    'arithmetics',
    'diff',
    # .coregistration
    'coregister',
    # .correlation
    'pairwise_var_correlation',
    'pixelwise_group_correlation',
    # data frame
    'add_dataset_values_to_geodataframe',
    'aggregate_statistics',
    'as_geodataframe',
    'data_frame_max',
    'data_frame_min',
    'data_frame_subset',
    'find_closest',
    'query',
    'to_dataframe',
    'to_dataset',
    # gapfill
    'gapfill',
    # .normalize
    'normalize',
    'adjust_temporal_attrs',
    'adjust_spatial_attrs',
    # .outliers
    'detect_outliers',
    # .plot
    'plot',
    'plot_contour',
    'plot_hist',
    'plot_line',
    'plot_map',
    'plot_scatter',
    # .resampling
    'resample',
    # .select
    'select_features',
    'select_var',
    # .subset
    'subset_spatial',
    'subset_temporal',
    'subset_temporal_index',
    # .timeseries
    'fourier_analysis',
    'tseries_point',
    'tseries_mean',
    # .utility
    'merge',
    'normalise_vars',
    'standardise_vars'
]
