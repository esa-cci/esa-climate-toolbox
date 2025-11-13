"""
Test Utility operations
"""

from datetime import timezone
from unittest import TestCase

import numpy as np
import numpy.testing as nt
import pandas as pd
import xarray as xr

from esa_climate_toolbox.core.types import ValidationError
from esa_climate_toolbox.ops.utility import merge
from esa_climate_toolbox.ops.utility import normalise_vars
from esa_climate_toolbox.ops.utility import standardise_vars
from xcube.core.new import new_cube


class MergeTest(TestCase):
    def test_nominal(self):
        """
        Test nominal execution
        """
        periods = 5
        time = pd.date_range('2000-01-01', periods=periods, tz=timezone.utc)

        ds_1 = xr.Dataset({'A': (['time'], np.random.randn(periods)),
                           'B': (['time'], np.random.randn(periods)),
                           'time': time})
        ds_2 = xr.Dataset({'C': (['time'], np.random.randn(periods)),
                           'D': (['time'], np.random.randn(periods)),
                           'time': time})
        new_ds = merge(ds_1=ds_1, ds_2=ds_2, ds_3=None, ds_4=None)
        self.assertTrue('A' in new_ds)
        self.assertTrue('B' in new_ds)
        self.assertTrue('C' in new_ds)
        self.assertTrue('D' in new_ds)

        new_ds = merge(ds_1=ds_1, ds_2=ds_1, ds_3=ds_1, ds_4=ds_2)
        self.assertTrue('A' in new_ds)
        self.assertTrue('B' in new_ds)
        self.assertTrue('C' in new_ds)
        self.assertTrue('D' in new_ds)

        new_ds = merge(ds_1=ds_1, ds_2=ds_1, ds_3=ds_1, ds_4=ds_1)
        self.assertIs(new_ds, ds_1)

        new_ds = merge(ds_1=ds_2, ds_2=ds_2, ds_3=ds_2, ds_4=ds_2)
        self.assertIs(new_ds, ds_2)

        ds_3 = xr.Dataset({'E': (['time'], np.random.randn(periods)),
                           'time': time})
        new_ds = merge(ds_1=ds_1, ds_2=ds_2, ds_3=ds_3, ds_4=None)
        self.assertTrue('A' in new_ds)
        self.assertTrue('B' in new_ds)
        self.assertTrue('C' in new_ds)
        self.assertTrue('D' in new_ds)
        self.assertTrue('E' in new_ds)

        ds_4 = xr.Dataset({'F': (['time'], np.random.randn(periods)),
                           'time': time})
        new_ds = merge(ds_1=ds_1, ds_2=ds_2, ds_3=ds_3, ds_4=ds_4)
        self.assertTrue('A' in new_ds)
        self.assertTrue('B' in new_ds)
        self.assertTrue('C' in new_ds)
        self.assertTrue('D' in new_ds)
        self.assertTrue('E' in new_ds)

    def test_failures(self):
        with self.assertRaises(ValidationError):
            merge(ds_1=None, ds_2=None, ds_3=None, ds_4=None)

def _sst_func(t, y, x):
    return t * 2.0 + x * 0.1

def _chl_func(t, y, x):
    return y * 1.0 + x * 0.1

def _aot_func(t, y, x):
    return y * 0.1 + t * 1.0 + x * 0.1

class NormaliseVarsTest(TestCase):

    def test_normalise_vars(self):
        cube = new_cube(
            width=4, height=2, time_periods=2, variables=dict(sst=_sst_func, chl=_chl_func, aot=_aot_func)
        )

        normalised_cube = normalise_vars(cube)

        self.assertIsNotNone(normalised_cube)
        expected_sst = np.array(
            [[[0., 0.05, 0.1, 0.15], [0., 0.05, 0.1, 0.15]],
             [[1., 1.05, 1.1, 1.15], [1., 1.05, 1.1, 1.15]]]
        )
        expected_chl = np.array(
            [[[np.nan, np.inf, np.inf, np.inf], [np.inf, np.inf, np.inf, np.inf]],
             [[np.nan, np.inf, np.inf, np.inf], [np.inf, np.inf, np.inf, np.inf]]]
        )
        expected_aot = np.array(
            [[[0., 0.1, 0.2, 0.3], [0.1, 0.2, 0.3, 0.4]],
            [[1., 1.1, 1.2, 1.3], [1.1, 1.2, 1.3, 1.4]]]
        )
        nt.assert_array_almost_equal(expected_sst, normalised_cube.sst.values)
        nt.assert_array_almost_equal(expected_chl, normalised_cube.chl.values)
        nt.assert_array_almost_equal(expected_aot, normalised_cube.aot.values)


class StandardiseVarsTest(TestCase):

    def test_standardise_vars(self):
        cube = new_cube(
            width=4, height=2, time_periods=2, variables=dict(sst=_sst_func, chl=_chl_func, aot=_aot_func)
        )

        standardised_cube = standardise_vars(cube)

        self.assertIsNotNone(standardised_cube)
        expected_sst = np.array(
            [[[-1., -1., -1., -1.], [-1., -1., -1., - 1.]],
             [[1.,  1.,  1., 1.], [1., 1., 1., 1.]]]
        )
        expected_chl = np.array(
            [[[np.nan, np.nan, np.nan, np.nan], [np.nan, np.nan, np.nan, np.nan]],
             [[np.nan, np.nan, np.nan, np.nan], [np.nan, np.nan, np.nan, np.nan]]]
        )
        expected_aot = np.array(
            [[[-1., -1., -1., -1.], [-1., -1., -1., -1.]],
             [[1., 1., 1., 1.], [1., 1., 1., 1.]]]
        )
        nt.assert_array_almost_equal(expected_sst, standardised_cube.sst.values)
        nt.assert_array_almost_equal(expected_chl, standardised_cube.chl.values)
        nt.assert_array_almost_equal(expected_aot, standardised_cube.aot.values)
