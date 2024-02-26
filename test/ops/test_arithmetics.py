"""
Tests for arithmetic operations
"""

from unittest import TestCase

from datetime import datetime
import numpy as np
import xarray as xr

from esa_climate_toolbox.core.op import OP_REGISTRY
from esa_climate_toolbox.ops import arithmetics
from esa_climate_toolbox.ops import diff
from esa_climate_toolbox.util.misc import object_to_qualified_name


def assert_dataset_equal(expected, actual):
    # this method is functionally equivalent to
    # `assert expected == actual`, but it checks each aspect
    # of equality separately for easier debugging
    assert expected.equals(actual), (expected, actual)


class TestDsArithmetics(TestCase):
    """
    Test dataset arithmetic operations
    """

    def test_nominal(self):
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        actual = arithmetics(dataset, '+2, -2, *3, /3, *4')
        assert_dataset_equal(expected * 4, actual)

        actual = arithmetics(
            dataset, 'exp, log, *10, log10, *2, log2, +1.5'
        )
        assert_dataset_equal(expected * 2.5, actual)

        actual = arithmetics(dataset, 'exp, -1, log1p, +3')
        assert_dataset_equal(expected * 4, actual)

        with self.assertRaises(ValueError) as err:
            arithmetics(dataset, 'not')
        self.assertTrue('not implemented' in str(err.exception))

    def test_registered(self):
        """
        Test the operation when invoked through the OP_REGISTRY
        """
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(arithmetics))
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        actual = reg_op(ds=dataset, ops='+2, -2, *3, /3, *4')
        assert_dataset_equal(expected * 4, actual)


class TestDiff(TestCase):
    """
    Test taking the difference between two datasets
    """

    def test_diff(self):
        # Test nominal
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})
        actual = diff(dataset, dataset * 2)
        assert_dataset_equal(expected * -1, actual)

        # Test variable mismatch
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'third': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        ds1 = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 3])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        actual = diff(ds, ds1)
        assert_dataset_equal(expected, actual)
        actual = diff(ds1, ds)
        assert_dataset_equal(expected, actual)

        # Test date range mismatch
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1, 13)]})

        ds1 = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2003, x, 1) for x in range(1, 13)]})

        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        actual = diff(ds, ds1)
        assert_dataset_equal(actual, expected)

        actual = diff(ds, ds1.drop_vars(['time', ]))
        expected['time'] = [datetime(2000, x, 1) for x in range(1, 13)]
        assert_dataset_equal(actual, expected)

        # Test broadcasting
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        ds1 = xr.Dataset({
            'first': (['lat', 'lon'], np.ones([45, 90])),
            'second': (['lat', 'lon'], np.ones([45, 90])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 3])),
            'second': (['lat', 'lon', 'time'], np.zeros([45, 90, 3])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})
        actual = diff(ds, ds1)
        assert_dataset_equal(expected, actual)

        ds['time'] = [datetime(2000, x, 1) for x in range(1, 4)]
        expected['time'] = [datetime(2000, x, 1) for x in range(1, 4)]
        actual = diff(ds, ds1)
        assert_dataset_equal(expected, actual)

        ds1 = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 1])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 1])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2001, 1, 1)]})
        actual = diff(ds, ds1)
        assert_dataset_equal(expected, actual)

        ds1 = ds1.squeeze('time')
        ds1['time'] = 1
        actual = diff(ds, ds1)
        assert_dataset_equal(expected, actual)

    def test_registered(self):
        """
        Test the operation when invoked from the OP_REGISTRY
        """
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(diff))
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})
        actual = reg_op(ds=dataset, ds2=dataset * 2)
        assert_dataset_equal(expected * -1, actual)
