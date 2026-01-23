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
import xarray as xr
from numpy.testing import assert_array_equal


from esa_climate_toolbox.ops import temporal_alignment


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

    ### Downsampling

    def test_month_to_year_overlap_at_primary_beginning(self):
        primary_ds = get_test_dataset(
            time_periods=5, time_freq="YS", time_start="2010-01-01T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=10, time_freq="MS", time_start="2009-08-01T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
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
            time_periods=5, time_freq="YS", time_start="2010-01-01T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=10, time_freq="MS", time_start="2011-08-01T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
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
            time_periods=5, time_freq="MS", time_start="2011-09-01T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=10, time_freq="D", time_start="2011-08-25T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        assert_array_equal(
            ds.r.values,
            [[[16, 16.5], [17, 17.5]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]]]
        )

    def test_day_to_month_full_overlap(self):
        primary_ds = get_test_dataset(
            time_periods=5, time_freq="MS", time_start="2011-07-01T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=10, time_freq="D", time_start="2011-09-26T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        assert_array_equal(
            ds.r.values,
            [[[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[4, 4.5], [5, 5.5]],
             [[14, 14.5], [15, 15.5]],
             [[np.nan, np.nan], [np.nan, np.nan]]]
        )

    def test_day_to_month_overlap_at_primary_end(self):
        primary_ds = get_test_dataset(
            time_periods=5, time_freq="MS", time_start="2011-05-01T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=10, time_freq="D", time_start="2011-09-26T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        assert_array_equal(
            ds.r.values,
            [[[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[4, 4.5], [5, 5.5]]]
        )

    def test_day_to_5day_overlap_at_primary_beginning(self):
        primary_ds = get_test_dataset(
            time_periods=5, time_freq="5D", time_start="2011-09-01T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=10, time_freq="D", time_start="2011-08-25T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        assert_array_equal(
            ds.r.values,
            [[[16, 16.5], [17, 17.5]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]]]
        )

    def test_day_to_5day_full_overlap(self):
        primary_ds = get_test_dataset(
            time_periods=5, time_freq="5D", time_start="2011-09-01T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=10, time_freq="D", time_start="2011-09-08T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        assert_array_equal(
            ds.r.values,
            [[[np.nan, np.nan], [np.nan, np.nan]],
             [[1, 1.5], [2, 2.5]],
             [[8, 8.5], [9, 9.5]],
             [[16, 16.5], [17, 17.5]],
             [[np.nan, np.nan], [np.nan, np.nan]]]
        )

    def test_day_to_5day_overlap_at_primary_end(self):
        primary_ds = get_test_dataset(
            time_periods=5, time_freq="5D", time_start="2011-09-01T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=10, time_freq="D", time_start="2011-09-21T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        assert_array_equal(
            ds.r.values,
            [[[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[4, 4.5], [5, 5.5]]]
        )

    def test_5day_to_month_overlap_at_primary_beginning(self):
        primary_ds = get_test_dataset(
            time_periods=5, time_freq="MS", time_start="2011-09-01T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=10, time_freq="5D", time_start="2011-08-15T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        assert_array_equal(
            ds.r.values,
            [[[11., 11.5], [12., 12.5]],
             [[18., 18.5], [19., 19.5]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]]]
        )

    def test_5day_to_month_full_overlap(self):
        primary_ds = get_test_dataset(
            time_periods=5, time_freq="MS", time_start="2011-07-01T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=10, time_freq="5D", time_start="2011-08-15T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        assert_array_equal(
            ds.r.values,
            [[[np.nan, np.nan], [np.nan, np.nan]],
             [[2., 2.5], [3., 3.5]],
             [[11., 11.5], [12., 12.5]],
             [[18., 18.5], [19., 19.5]],
             [[np.nan, np.nan], [np.nan, np.nan]]]
        )

    def test_5day_to_month_overlap_at_primary_end(self):
        primary_ds = get_test_dataset(
            time_periods=5, time_freq="MS", time_start="2011-05-01T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=10, time_freq="5D", time_start="2011-08-15T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        assert_array_equal(
            ds.r.values,
            [[[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[2., 2.5], [3., 3.5]],
             [[11., 11.5], [12., 12.5]]]
        )

    def test_3hour_to_day_overlap_at_primary_beginning(self):
        primary_ds = get_test_dataset(
            time_periods=5, time_freq="D", time_start="2011-08-18T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=10, time_freq="6H", time_start="2011-08-15T21:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        assert_array_equal(
            ds.r.values,
            [[[17., 17.5], [18., 18.5]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]]]
        )

    def test_3hour_to_day_full_overlap(self):
        primary_ds = get_test_dataset(
            time_periods=5, time_freq="D", time_start="2011-08-14T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=10, time_freq="6H", time_start="2011-08-15T03:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        assert_array_equal(
            ds.r.values,
            [[[np.nan, np.nan], [np.nan, np.nan]],
             [[3., 3.5], [4., 4.5]],
             [[11., 11.5], [12., 12.5]],
             [[17., 17.5], [18., 18.5]],
             [[np.nan, np.nan], [np.nan, np.nan]]]
        )

    def test_3hour_to_day_overlap_at_primary_end(self):
        primary_ds = get_test_dataset(
            time_periods=5, time_freq="D", time_start="2011-08-11T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=10, time_freq="6H", time_start="2011-08-15T3:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        assert_array_equal(
            ds.r.values,
            [[[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[3., 3.5], [4., 4.5]]]
        )

    def test_2week_to_quarter_overlap_at_primary_beginning(self):
        primary_ds = get_test_dataset(
            time_periods=5, time_freq="QS", time_start="2011-01-01T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=10, time_freq="2W", time_start="2010-10-10T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        assert_array_equal(
            ds.r.values,
            [[[15., 15.5], [16., 16.5]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]]]
        )

    def test_2week_to_quarter_full_overlap(self):
        primary_ds = get_test_dataset(
            time_periods=5, time_freq="QS", time_start="2011-01-01T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=10, time_freq="2W", time_start="2011-05-10T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        assert_array_equal(
            ds.r.values,
            [[[np.nan, np.nan], [np.nan, np.nan]],
             [[2., 2.5], [3, 3.5]],
             [[12., 12.5], [13., 13.5]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]]]
        )

    def test_2week_to_quarter_overlap_at_primary_end(self):
        primary_ds = get_test_dataset(
            time_periods=5, time_freq="QS", time_start="2010-01-01T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=10, time_freq="2W", time_start="2011-03-01T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        assert_array_equal(
            ds.r.values,
            [[[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[1., 1.5], [2, 2.5]]]
        )

    ### Upsampling

    def test_year_to_month_overlap_at_primary_beginning(self):
        primary_ds = get_test_dataset(
            time_periods=10, time_freq="M", time_start="2015-07-01T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=5, time_freq="Y", time_start="2010-01-01T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds, method="nearest")

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        assert_array_equal(
            ds.r.values,
            [[[8, 8.5], [9, 9.5]],
             [[8, 8.5], [9, 9.5]],
             [[8, 8.5], [9, 9.5]],
             [[8, 8.5], [9, 9.5]],
             [[8, 8.5], [9, 9.5]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]]]
        )

    def test_year_to_month_full_overlap(self):
        primary_ds = get_test_dataset(
            time_periods=10, time_freq="MS", time_start="2012-07-01T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=5, time_freq="YS", time_start="2010-01-01T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        assert_array_equal(
            ds.r.values,
            [[[4, 4.5], [5, 5.5]],
             [[4, 4.5], [5, 5.5]],
             [[4, 4.5], [5, 5.5]],
             [[4, 4.5], [5, 5.5]],
             [[4, 4.5], [5, 5.5]],
             [[4, 4.5], [5, 5.5]],
             [[6, 6.5], [7, 7.5]],
             [[6, 6.5], [7, 7.5]],
             [[6, 6.5], [7, 7.5]],
             [[6, 6.5], [7, 7.5]]]
        )

    def test_year_to_month_at_primary_end(self):
        primary_ds = get_test_dataset(
            time_periods=10, time_freq="MS", time_start="2009-07-01T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=5, time_freq="YS", time_start="2010-01-01T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        assert_array_equal(
            ds.r.values,
            [[[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[0, 0.5], [1, 1.5]],
             [[0, 0.5], [1, 1.5]],
             [[0, 0.5], [1, 1.5]],
             [[0, 0.5], [1, 1.5]]]
        )

    def test_5day_to_day_overlap_at_primary_beginning(self):
        primary_ds = get_test_dataset(
            time_periods=10, time_freq="D", time_start="2015-06-22T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=5, time_freq="5D", time_start="2015-06-01T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds, method="nearest")

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        assert_array_equal(
            ds.r.values,
            [[[8, 8.5], [9, 9.5]],
             [[8, 8.5], [9, 9.5]],
             [[8, 8.5], [9, 9.5]],
             [[8, 8.5], [9, 9.5]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]]]
        )

    def test_5day_to_day_full_overlap(self):
        primary_ds = get_test_dataset(
            time_periods=10, time_freq="D", time_start="2015-06-12T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=5, time_freq="5D", time_start="2015-06-01T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        assert_array_equal(
            ds.r.values,
            [[[4, 4.5], [5, 5.5]],
             [[4, 4.5], [5, 5.5]],
             [[4, 4.5], [5, 5.5]],
             [[4, 4.5], [5, 5.5]],
             [[6, 6.5], [7, 7.5]],
             [[6, 6.5], [7, 7.5]],
             [[6, 6.5], [7, 7.5]],
             [[6, 6.5], [7, 7.5]],
             [[6, 6.5], [7, 7.5]],
             [[8, 8.5], [9, 9.5]]]
        )

    def test_5day_to_day_at_primary_end(self):
        primary_ds = get_test_dataset(
            time_periods=10, time_freq="D", time_start="2015-06-02T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=5, time_freq="5D", time_start="2015-06-11T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        assert_array_equal(
            ds.r.values,
            [[[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[0, 0.5], [1, 1.5]]]
        )

    def test_quarter_to_2week_overlap_at_primary_beginning(self):
        primary_ds = get_test_dataset(
            time_periods=10, time_freq="2W", time_start="2016-02-17T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=5, time_freq="QS", time_start="2015-01-01T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds, method="nearest")

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        assert_array_equal(
            ds.r.values,
            [[[8, 8.5], [9, 9.5]],
             [[8, 8.5], [9, 9.5]],
             [[8, 8.5], [9, 9.5]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]]]
        )

    def test_quarter_to_2week_full_overlap(self):
        primary_ds = get_test_dataset(
            time_periods=10, time_freq="2W", time_start="2016-02-17T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=5, time_freq="QS", time_start="2015-07-01T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        assert_array_equal(
            ds.r.values,
            [[[4, 4.5], [5, 5.5]],
             [[4, 4.5], [5, 5.5]],
             [[4, 4.5], [5, 5.5]],
             [[6, 6.5], [7, 7.5]],
             [[6, 6.5], [7, 7.5]],
             [[6, 6.5], [7, 7.5]],
             [[6, 6.5], [7, 7.5]],
             [[6, 6.5], [7, 7.5]],
             [[6, 6.5], [7, 7.5]],
             [[8, 8.5], [9, 9.5]]]
        )

    def test_quarter_to_2week_at_primary_end(self):
        primary_ds = get_test_dataset(
            time_periods=10, time_freq="2W", time_start="2015-03-17T00:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=5, time_freq="QS", time_start="2015-07-01T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        assert_array_equal(
            ds.r.values,
            [[[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[0, 0.5], [1, 1.5]],
             [[0, 0.5], [1, 1.5]],
             [[0, 0.5], [1, 1.5]]]
        )

    def test_2day_to_9hour_overlap_at_primary_beginning(self):
        primary_ds = get_test_dataset(
            time_periods=10, time_freq="10H", time_start="2015-07-25T19:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=5, time_freq="2D", time_start="2015-07-18T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds, method="nearest")

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        assert_array_equal(
            ds.r.values,
            [[[6, 6.5], [7, 7.5]],
             [[8, 8.5], [9, 9.5]],
             [[8, 8.5], [9, 9.5]],
             [[8, 8.5], [9, 9.5]],
             [[8, 8.5], [9, 9.5]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]]]
        )

    def test_2day_to_9hour_full_overlap(self):
        primary_ds = get_test_dataset(
            time_periods=10, time_freq="10H", time_start="2015-07-25T19:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=5, time_freq="2D", time_start="2015-07-22T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        assert_array_equal(
            ds.r.values,
            [[[2, 2.5], [3, 3.5]],
             [[4, 4.5], [5, 5.5]],
             [[4, 4.5], [5, 5.5]],
             [[4, 4.5], [5, 5.5]],
             [[4, 4.5], [5, 5.5]],
             [[6, 6.5], [7, 7.5]],
             [[6, 6.5], [7, 7.5]],
             [[6, 6.5], [7, 7.5]],
             [[6, 6.5], [7, 7.5]],
             [[6, 6.5], [7, 7.5]]]
        )

    def test_2day_to_9hour_at_primary_end(self):
        primary_ds = get_test_dataset(
            time_periods=10, time_freq="10H", time_start="2015-07-19T19:00:00", var_name="p")
        replica_ds = get_test_dataset(
            time_periods=5, time_freq="2D", time_start="2015-07-22T00:00:00", var_name="r")

        ds = temporal_alignment(primary_ds, replica_ds)

        self.assertIsNotNone(ds)
        assert_array_equal(ds.time.values, primary_ds.time.values)
        assert_array_equal(
            ds.r.values,
            [[[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[np.nan, np.nan], [np.nan, np.nan]],
             [[0, 0.5], [1, 1.5]],
             [[0, 0.5], [1, 1.5]],
             [[0, 0.5], [1, 1.5]],
             [[0, 0.5], [1, 1.5]],
             [[0, 0.5], [1, 1.5]]]
        )
