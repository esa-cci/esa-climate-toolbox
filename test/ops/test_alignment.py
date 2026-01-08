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

from unittest import TestCase
from xcube.core.new import new_cube

import numpy as np
# import numpy.testing as npt
import xarray as xr
from numpy.testing import assert_almost_equal, assert_array_equal

from esa_climate_toolbox.core.op import OP_REGISTRY
from esa_climate_toolbox.util.misc import object_to_qualified_name

from esa_climate_toolbox.ops import temporal_alignment
from esa_climate_toolbox.ops.coregistration import _find_intersection
from ..util.test_monitor import RecordingMonitor


def get_test_dataset(time_periods: int, time_freq: str, time_start: str, var_name: str):
    inner_grid = np.arange(2 * 2).reshape(2, 2) * 0.5
    outer_offsets = np.arange(time_periods).reshape(time_periods, 1, 1) * 2
    array = xr.DataArray(inner_grid + outer_offsets, dims=("time", "lat", "lon"), name=var_name).chunk()
    return new_cube(
        time_periods=time_periods,
        time_freq=time_freq,
        width=2,
        height=2,
        time_start=time_start,
        variables={
            var_name: array
        },
        drop_bounds=True
    )


class TestTemporalAlignment(TestCase):

    # def test_downsampling_to_coarser_subset(self):
    #     primary_ds = get_test_dataset(
    #         time_periods=5,
    #         time_freq="W",
    #         time_start="2010-01-01T00:00:00",
    #         var_name="p"
    #     )
    #     replica_ds = get_test_dataset(
    #         time_periods=10,
    #         time_freq="D",
    #         time_start="2010-01-10T00:00:00",
    #         var_name="r"
    #     )
    #     ds = temporal_alignment(
    #         primary_ds, replica_ds
    #     )
    #     self.assertIsNotNone(ds)

    def test_month_to_year_overlap_at_primary_beginning(self):
        primary_ds = get_test_dataset(
            time_periods=5, time_freq="Y", time_start="2009-01-01T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=10, time_freq="M", time_start="2009-07-01T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        # assert_array_equal(ds.time_bnds.values, primary_ds.time_bnds.values)
        assert_array_equal(
            ds.r.values,
            [[[14, 14.5], [15, 15.5]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]]]
        )

    def test_month_to_year_full_overlap(self):
        primary_ds = get_test_dataset(
            time_periods=5, time_freq="Y", time_start="2010-01-01T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=10, time_freq="M", time_start="2012-07-01T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        # assert_array_equal(ds.time_bnds.values, primary_ds.time_bnds.values)
        assert_array_equal(
            ds.r.values,
            [[[np.nan, np.nan], [np.nan, np.nan]],
             [[4, 4.5], [5, 5.5]],
             [[14, 14.5], [15, 15.5]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]]]
        )

    def test_month_to_year_overlap_at_primary_end(self):
        primary_ds = get_test_dataset(
            time_periods=5, time_freq="Y", time_start="2010-01-01T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=10, time_freq="M", time_start="2015-07-01T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        # assert_array_equal(ds.time_bnds.values, primary_ds.time_bnds.values)
        assert_array_equal(
            ds.r.values,
            [[[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[4, 4.5], [5, 5.5]]]
        )

    def test_day_to_month_overlap_at_primary_beginning(self):
        primary_ds = get_test_dataset(
            time_periods=5, time_freq="M", time_start="2011-09-01T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=10, time_freq="D", time_start="2011-09-25T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        assert_array_equal(ds.time_bnds.values, primary_ds.time_bnds.values)
        assert_array_equal(
            ds.r.values,
            [[[14, 14.5], [15, 15.5]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]]]
        )

    def test_day_to_month_full_overlap(self):
        primary_ds = get_test_dataset(
            time_periods=5, time_freq="M", time_start="2011-07-01T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=10, time_freq="D", time_start="2011-09-25T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        # assert_array_equal(ds.time_bnds.values, primary_ds.time_bnds.values)
        assert_array_equal(
            ds.r.values,
            [[[np.nan, np.nan], [np.nan, np.nan]],
             [[4, 4.5], [5, 5.5]],
             [[14, 14.5], [15, 15.5]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]]]
        )

    def test_day_to_month_overlap_at_primary_end(self):
        primary_ds = get_test_dataset(
            time_periods=5, time_freq="M", time_start="2011-04-01T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=10, time_freq="D", time_start="2011-09-25T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        assert_array_equal(ds.time_bnds.values, primary_ds.time_bnds.values)
        assert_array_equal(
            ds.r.values,
            [[[14, 14.5], [15, 15.5]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]]]
        )

    def test_year_to_month_overlap_at_primary_beginning(self):
        primary_ds = get_test_dataset(
            time_periods=10, time_freq="M", time_start="2014-07-01T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=5, time_freq="Y", time_start="2009-01-01T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds, method="nearest")

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        # assert_array_equal(ds.time_bnds.values, primary_ds.time_bnds.values)
        assert_array_equal(
            ds.r.values,
            [[[14, 14.5], [15, 15.5]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]]]
        )

    def test_month_to_year_full_overlap(self):
        primary_ds = get_test_dataset(
            time_periods=5, time_freq="Y", time_start="2010-01-01T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=10, time_freq="M", time_start="2012-07-01T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        assert_array_equal(ds.time_bnds.values, primary_ds.time_bnds.values)
        assert_array_equal(
            ds.r.values,
            [[[np.nan, np.nan], [np.nan, np.nan]],
             [[4, 4.5], [5, 5.5]],
             [[14, 14.5], [15, 15.5]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]]]
        )

    def test_month_to_year_overlap_at_primary_end(self):
        primary_ds = get_test_dataset(
            time_periods=5, time_freq="Y", time_start="2010-01-01T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=10, time_freq="M", time_start="2015-07-01T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        assert_array_equal(ds.time_bnds.values, primary_ds.time_bnds.values)
        assert_array_equal(
            ds.r.values,
            [[[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[4, 4.5], [5, 5.5]]]
        )
